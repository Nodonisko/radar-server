"""Benchmark forecast field generation against local CZ CHMI fixtures.

Run from the repository root:

    python -m radar_server.benchmark_forecast_generation

By default the script loads and prepares history fields once, then times only
``generate_forecast_fields`` so rendering and repeated HDF decoding stay out of
the baseline.
"""

from __future__ import annotations

import argparse
import gc
import json
import statistics
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from . import config as radar_config
from .config import ForecastProduct, InputConfig, ProductConfig, cz_forecast
from .forecast_generation import generate_forecast_fields, load_history_fields
from .logging_config import configure_logging
from .queueing import ForecastGenTask, HistoryFrame
from .rendering.core import RadarField


@dataclass(frozen=True)
class FixtureFrame:
    timestamp: datetime
    paths: tuple[Path, ...]


def main() -> int:
    args = _parse_args()
    configure_logging(args.log_level)

    minutes = args.minutes
    forecast = _forecast_by_id(args.forecast_id)
    data_dirs = _parse_data_dirs(args.data_dir)
    parent = _with_data_dirs(forecast.parent, data_dirs)
    forecast = replace(forecast, parent=parent)
    motion_grid_step = args.motion_grid_step if args.motion_grid_step is not None else forecast.motion_grid_step
    motion_grid_max = args.motion_grid_max if args.motion_grid_max is not None else forecast.motion_grid_max
    fast_idw = args.fast_idw if args.fast_idw is not None else forecast.fast_idw
    fast_warp = args.fast_warp if args.fast_warp is not None else forecast.fast_warp
    fast_motion = args.fast_motion if args.fast_motion is not None else forecast.fast_motion
    warp_grid_step = args.warp_grid_step if args.warp_grid_step is not None else forecast.warp_grid_step
    frames = _select_history_frames(
        forecast.parent,
        history_frames=args.history_frames,
        issue_timestamp=args.issue_timestamp,
    )
    task = ForecastGenTask(
        forecast=forecast,
        issue_timestamp=frames[-1].timestamp,
        history=tuple(HistoryFrame(frame.timestamp, frame.paths) for frame in frames),
    )

    fields, load_seconds = _timed_load(task)
    _validate_fields(fields)

    options = dict(
        minutes=minutes,
        method=args.method,
        floor_level=forecast.palette.levels[0],
        motion_grid_step=motion_grid_step,
        motion_grid_max=motion_grid_max,
        fast_idw=fast_idw,
        fast_warp=fast_warp,
        fast_motion=fast_motion,
        warp_grid_step=warp_grid_step,
    )
    runs = _run_benchmark(
        fields,
        options=options,
        warmups=args.warmups,
        iterations=args.iterations,
        include_load=args.include_load,
        task=task,
        disable_gc=not args.keep_gc,
    )

    summary = _summarize(
        frames=frames,
        fields=fields,
        forecast=forecast,
        options=options,
        warmups=args.warmups,
        load_seconds=load_seconds,
        runs=runs,
        include_load=args.include_load,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_summary(summary)
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m radar_server.benchmark_forecast_generation",
        description="Benchmark forecast generation using local HDF fixtures.",
    )
    parser.add_argument(
        "--forecast-id",
        default=cz_forecast.id,
        help=f"Forecast config ID to benchmark (default: {cz_forecast.id}).",
    )
    parser.add_argument(
        "--data-dir",
        action="append",
        default=(),
        help=(
            "Override input fixture directory. For multi-input products use input_id=path; "
            "can be passed multiple times."
        ),
    )
    parser.add_argument(
        "--issue-timestamp",
        type=_parse_timestamp,
        default=None,
        help="Use the latest history ending at or before this UTC timestamp (YYYY-mm-ddTHH:MM:SS).",
    )
    parser.add_argument(
        "--history-frames",
        type=int,
        default=cz_forecast.history_frames,
        help=f"Number of source frames to use (default: {cz_forecast.history_frames})",
    )
    parser.add_argument(
        "--minutes",
        type=_parse_minutes,
        default=cz_forecast.minutes,
        help="Comma-separated forecast lead minutes.",
    )
    parser.add_argument(
        "--method",
        default=cz_forecast.method,
        help=f"pysteps motion method (default: {cz_forecast.method})",
    )
    parser.add_argument(
        "--motion-grid-step",
        type=int,
        default=None,
        help=(
            "Coarsen factor for densifying the LK motion field (1 = full resolution). "
            "Defaults to the forecast's configured value."
        ),
    )
    parser.add_argument(
        "--motion-grid-max",
        type=int,
        default=None,
        help="Cap the motion-interpolation grid's longest edge (pixels). Defaults to the forecast's value.",
    )
    parser.add_argument(
        "--warp-grid-step",
        type=int,
        default=None,
        help=(
            "Coarsen factor for the semi-Lagrangian warp's trajectory integration "
            "(1 = full resolution; only used with fast warp). Defaults to the forecast's value."
        ),
    )
    parser.add_argument(
        "--fast-idw",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use the parallel kd-tree IDW (forecast_fast). Defaults to the forecast's value.",
    )
    parser.add_argument(
        "--fast-warp",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use the cv2.remap semi-Lagrangian extrapolation (forecast_fast). Defaults to the forecast's value.",
    )
    parser.add_argument(
        "--fast-motion",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use the MaskedArray-free sparse Lucas-Kanade path (forecast_fast). Defaults to the forecast's value.",
    )
    parser.add_argument("--warmups", type=int, default=1, help="Untimed warmup runs before measuring.")
    parser.add_argument("--iterations", type=int, default=5, help="Measured iterations.")
    parser.add_argument(
        "--include-load",
        action="store_true",
        help="Measure load/prep plus generation each iteration instead of generation only.",
    )
    parser.add_argument("--keep-gc", action="store_true", help="Leave Python garbage collection enabled.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument(
        "--log-level",
        default="WARNING",
        help="Python logging level. Use INFO to see forecast_generation phase timings.",
    )
    return parser.parse_args()


def _parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _parse_minutes(value: str) -> tuple[int, ...]:
    try:
        minutes = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid lead minutes: {value!r}") from exc
    if not minutes:
        raise argparse.ArgumentTypeError("at least one lead minute is required")
    if any(minute <= 0 for minute in minutes):
        raise argparse.ArgumentTypeError(f"lead minutes must be positive: {minutes!r}")
    return minutes


def _forecast_by_id(forecast_id: str) -> ForecastProduct:
    forecasts = {
        value.id: value
        for value in vars(radar_config).values()
        if isinstance(value, ForecastProduct)
    }
    try:
        return forecasts[forecast_id]
    except KeyError as exc:
        available = ", ".join(sorted(forecasts))
        raise ValueError(f"unknown forecast ID {forecast_id!r}; available: {available}") from exc


def _parse_data_dirs(values: Sequence[str]) -> dict[str | None, Path]:
    data_dirs: dict[str | None, Path] = {}
    for value in values:
        if "=" in value:
            input_id, raw_path = value.split("=", 1)
            data_dirs[input_id] = Path(raw_path)
        else:
            data_dirs[None] = Path(value)
    return data_dirs


def _with_data_dirs(product: ProductConfig, data_dirs: dict[str | None, Path]) -> ProductConfig:
    if not data_dirs:
        return product
    inputs: list[InputConfig] = []
    for input_config in product.inputs:
        override = data_dirs.get(input_config.id, data_dirs.get(None))
        inputs.append(replace(input_config, local_dir=override) if override is not None else input_config)
    return replace(product, inputs=tuple(inputs))


def _select_history_frames(
    product: ProductConfig,
    *,
    history_frames: int,
    issue_timestamp: datetime | None,
) -> list[FixtureFrame]:
    if history_frames < 2:
        raise ValueError(f"forecast benchmark needs at least 2 history frames, got {history_frames}")

    grouped_by_input = {input_config.id: _scan_input_paths(input_config) for input_config in product.inputs}
    if not grouped_by_input:
        raise ValueError(f"product {product.id!r} has no configured inputs")

    ready = set.intersection(*(set(grouped) for grouped in grouped_by_input.values()))
    candidates = []
    for timestamp in sorted(ready):
        if issue_timestamp is not None and timestamp > issue_timestamp:
            continue
        paths = tuple(
            path
            for input_config in product.inputs
            for path in grouped_by_input[input_config.id][timestamp]
        )
        candidates.append(FixtureFrame(timestamp=timestamp, paths=paths))
    if len(candidates) < history_frames:
        dirs = ", ".join(f"{input_config.id}={input_config.local_dir}" for input_config in product.inputs)
        raise ValueError(
            f"need {history_frames} usable fixture timestamps at or before {issue_timestamp}, "
            f"found {len(candidates)} for {product.id} in {dirs}"
        )
    return candidates[-history_frames:]


def _scan_input_paths(input_config: InputConfig) -> dict[datetime, tuple[Path, ...]]:
    if not input_config.local_dir.exists():
        raise FileNotFoundError(f"fixture directory does not exist: {input_config.local_dir}")

    grouped: dict[datetime, list[Path]] = {}
    for path in sorted(input_config.local_dir.iterdir()):
        if not path.is_file() or path.name.endswith(".part") or path.suffix.lower() not in input_config.file_suffixes:
            continue
        timestamp = input_config.timestamp_from_name(path.name)
        if timestamp is None:
            continue
        grouped.setdefault(timestamp, []).append(path)
    return {timestamp: tuple(sorted(paths)) for timestamp, paths in grouped.items()}


def _timed_load(task: ForecastGenTask) -> tuple[list[RadarField], float]:
    start = time.perf_counter()
    fields = load_history_fields(task)
    return fields, time.perf_counter() - start


def _validate_fields(fields: Sequence[RadarField]) -> None:
    if len(fields) < 2:
        raise ValueError(f"forecast benchmark needs at least 2 loaded fields, got {len(fields)}")
    if fields[-1].timestamp <= fields[-2].timestamp:
        raise ValueError("loaded fields are not chronological")
    if not np.isfinite(fields[-1].values).any():
        raise ValueError("latest loaded field contains no finite radar values")


def _run_benchmark(
    fields: Sequence[RadarField],
    *,
    options: dict[str, Any],
    warmups: int,
    iterations: int,
    include_load: bool,
    task: ForecastGenTask,
    disable_gc: bool,
) -> list[float]:
    if warmups < 0:
        raise ValueError(f"warmups must be non-negative, got {warmups}")
    if iterations <= 0:
        raise ValueError(f"iterations must be positive, got {iterations}")

    benchmarked = _load_and_generate if include_load else _generate
    for _ in range(warmups):
        benchmarked(task, fields, options)

    was_enabled = gc.isenabled()
    if disable_gc:
        gc.disable()
    try:
        runs: list[float] = []
        for _ in range(iterations):
            start = time.perf_counter()
            benchmarked(task, fields, options)
            runs.append(time.perf_counter() - start)
        return runs
    finally:
        if disable_gc and was_enabled:
            gc.enable()


def _generate(
    task: ForecastGenTask,
    fields: Sequence[RadarField],
    options: dict[str, Any],
) -> dict[int, RadarField]:
    return generate_forecast_fields(fields, forecast_id=task.forecast.id, **options)


def _load_and_generate(
    task: ForecastGenTask,
    fields: Sequence[RadarField],
    options: dict[str, Any],
) -> dict[int, RadarField]:
    del fields
    loaded = load_history_fields(task)
    return generate_forecast_fields(loaded, forecast_id=task.forecast.id, **options)


def _summarize(
    *,
    frames: Sequence[FixtureFrame],
    fields: Sequence[RadarField],
    forecast: ForecastProduct,
    options: dict[str, Any],
    warmups: int,
    load_seconds: float,
    runs: Sequence[float],
    include_load: bool,
) -> dict[str, Any]:
    latest = fields[-1]
    latest_values = latest.values
    finite_fraction = float(np.isfinite(latest_values).mean())
    return {
        "benchmark": "load+forecast" if include_load else "forecast_only",
        "forecast_id": forecast.id,
        "product_id": forecast.parent.id,
        "method": options["method"],
        "motion_grid_step": options["motion_grid_step"],
        "motion_grid_max": options["motion_grid_max"],
        "fast_idw": options["fast_idw"],
        "fast_warp": options["fast_warp"],
        "fast_motion": options["fast_motion"],
        "warp_grid_step": options["warp_grid_step"],
        "history_frames": len(fields),
        "lead_minutes": list(options["minutes"]),
        "warmups": warmups,
        "iterations": len(runs),
        "issue_timestamp": frames[-1].timestamp.isoformat(),
        "source_files": [path.name for frame in frames for path in frame.paths],
        "field_shape": list(latest_values.shape),
        "finite_fraction": finite_fraction,
        "one_time_load_ms": load_seconds * 1000,
        "runs_ms": [run * 1000 for run in runs],
        "min_ms": min(runs) * 1000,
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms": statistics.fmean(runs) * 1000,
        "stdev_ms": statistics.stdev(runs) * 1000 if len(runs) > 1 else 0.0,
        "max_ms": max(runs) * 1000,
    }


def _print_summary(summary: dict[str, Any]) -> None:
    print(f"Benchmark: {summary['benchmark']}")
    print(
        f"Method: {summary['method']} (motion_grid_step={summary['motion_grid_step']}, "
        f"motion_grid_max={summary['motion_grid_max']}, warp_grid_step={summary['warp_grid_step']}, "
        f"fast_motion={summary['fast_motion']}, fast_idw={summary['fast_idw']}, fast_warp={summary['fast_warp']})"
    )
    print(f"Issue timestamp: {summary['issue_timestamp']}")
    print(f"History frames: {summary['history_frames']}")
    print(f"Lead minutes: {summary['lead_minutes']}")
    print(f"Field shape: {summary['field_shape']} (finite {summary['finite_fraction']:.1%})")
    print(f"One-time load/prep: {summary['one_time_load_ms']:.1f} ms")
    print(
        "Measured: "
        f"median {summary['median_ms']:.1f} ms | "
        f"mean {summary['mean_ms']:.1f} ms | "
        f"min {summary['min_ms']:.1f} ms | "
        f"max {summary['max_ms']:.1f} ms | "
        f"stdev {summary['stdev_ms']:.1f} ms"
    )
    print("Runs: " + ", ".join(f"{run:.1f}" for run in summary["runs_ms"]) + " ms")


if __name__ == "__main__":
    raise SystemExit(main())
