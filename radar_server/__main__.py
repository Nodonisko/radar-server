"""Command line entry point for radar_server."""

from __future__ import annotations

import argparse
import logging
import signal
from dataclasses import replace

from .config import CONFIG, RadarServerConfig
from .fetching import download_remote_file
from .mqtt_watcher import MqttNotification, MqttWatcher
from .runtime import RadarRuntime, run_forecasts_once
from .scheduler import RadarScheduler, SchedulerCycleResult


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m radar_server")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_once = subparsers.add_parser("run-once", help="Fetch inputs and render ready products once")
    run_once.add_argument("--limit-per-input", type=int, default=None)
    run_once.add_argument("--no-optimize", action="store_true", help="Skip PNG optimization")
    run_once.add_argument("--no-forecast", action="store_true", help="Skip forecast generation")

    run = subparsers.add_parser("run", help="Run the queue-based runtime (MQTT + polling producers)")
    run.add_argument("--sleep-seconds", type=float, default=1.0)
    run.add_argument("--fallback-interval-seconds", type=int, default=300)
    run.add_argument("--backfill-interval-seconds", type=int, default=1800)
    run.add_argument("--mqtt-stale-seconds", type=int, default=600)
    run.add_argument("--no-optimize", action="store_true", help="Skip PNG optimization")

    poll = subparsers.add_parser("poll", help="Run the legacy synchronous polling scheduler forever")
    poll.add_argument("--sleep-seconds", type=float, default=1.0)
    poll.add_argument("--no-optimize", action="store_true", help="Skip PNG optimization")

    mqtt = subparsers.add_parser("mqtt", help="Run the MQTT watcher forever (downloads only; debug tool)")
    mqtt.add_argument("--no-optimize", action="store_true", help="Skip PNG optimization")

    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    config = _with_optimize(CONFIG, optimize=not args.no_optimize)

    if args.command == "run-once":
        scheduler = RadarScheduler(config)
        result = scheduler.run_once(limit_per_input=args.limit_per_input)
        _print_cycle_summary(result)
        if not args.no_forecast:
            rendered = run_forecasts_once(config)
            print(f"rendered forecast frames: {rendered}")
        return 1 if any(item.error is not None for item in result.synced) else 0

    if args.command == "run":
        runtime = RadarRuntime(config)
        signal.signal(signal.SIGTERM, lambda signum, frame: runtime.request_stop())
        runtime.run_forever(
            sleep_seconds=args.sleep_seconds,
            fallback_interval_seconds=args.fallback_interval_seconds,
            backfill_interval_seconds=args.backfill_interval_seconds,
            mqtt_stale_seconds=args.mqtt_stale_seconds,
        )
        return 0

    if args.command == "poll":
        scheduler = RadarScheduler(config)
        scheduler.run_forever(sleep_seconds=args.sleep_seconds)
        return 0

    if args.command == "mqtt":
        def download_notification(notification: MqttNotification) -> None:
            for remote in notification.remotes:
                download_remote_file(remote)

        MqttWatcher(inputs=config.inputs, on_notification=download_notification).run_forever()
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


def _with_optimize(config: RadarServerConfig, *, optimize: bool) -> RadarServerConfig:
    products = tuple(
        replace(product, render=replace(product.render, optimize=optimize))
        for product in config.products
    )
    # Forecast products link to parent product objects; remap them onto the
    # rebuilt products so optimize (and any other render setting) flows through.
    products_by_id = {product.id: product for product in products}
    forecasts = tuple(
        replace(forecast, parent=products_by_id.get(forecast.parent.id, forecast.parent))
        for forecast in config.forecasts
    )
    return replace(config, products=products, forecasts=forecasts)


def _print_cycle_summary(result: SchedulerCycleResult) -> None:
    print(f"synced inputs: {len(result.synced)}")
    print(f"downloaded files: {result.downloaded_count}")
    print(f"rendered products: {len(result.rendered)}")
    for sync_result in result.synced:
        if sync_result.error is not None:
            print(f"input {sync_result.input.id}: ERROR {sync_result.error}")
            continue
        downloaded = sum(1 for item in sync_result.files if item.downloaded)
        print(f"input {sync_result.input.id}: {len(sync_result.files)} files ({downloaded} downloaded)")
    for render_result in result.rendered:
        variants = ", ".join(path.name for path in render_result.variants.values())
        print(f"rendered {render_result.base}: {variants}")


if __name__ == "__main__":
    raise SystemExit(main())
