"""Queue-based runtime: networking threads orchestrate, workers do heavy work.

Thread layout:

- paho network thread: parses MQTT notifications and enqueues ingest tasks.
- main loop (this module): decides due polls, enqueues ingest tasks, gates the
  forecast generation pool on the render lane being idle, runs pruning.
- download worker (1 thread): downloads files, refreshes the input index,
  enqueues observed render tasks and forecast wants.
- render worker (1 thread): drains the render priority queue sequentially;
  lower priority numbers render first (cz before other countries before the
  composite before any forecast frame).
- forecast generation pool (2 threads): pysteps motion/extrapolation, writes
  fields to disk, then enqueues lowest-priority forecast render tasks.

The queues are in-memory; on startup :meth:`RadarRuntime.reconcile_startup`
rebuilds pending work from the filesystem (idempotent outputs), so shutdown
simply abandons the queues.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from threading import Event
from typing import Callable

from . import forecast_store
from .config import CONFIG, InputConfig, OrdApiSource, RadarServerConfig
from .input_index import LocalInputIndex
from .mqtt_watcher import MqttNotification, MqttWatcher
from .pruning import prune_all
from .queueing import ForecastRenderTask, MqttIngestTask, PollIngestTask, PriorityWorkQueue
from .r2_upload import R2UploadWorker
from .rendering.pipeline import OutputReadyCallback
from .scheduler import RadarScheduler, default_limit_per_input
from .workers import (
    DownloadWorker,
    ForecastGenPool,
    ForecastWantBoard,
    IndexHolder,
    RenderWorker,
    build_forecast_gen_task,
    enqueue_observed_render_jobs,
    update_forecast_wants,
)

LOGGER = logging.getLogger(__name__)

Sleep = Callable[[float], None]

INGEST_PRIORITY = 0


class RadarRuntime:
    """Wire MQTT and polling producers to the download/render/forecast workers."""

    def __init__(
        self,
        config: RadarServerConfig = CONFIG,
        *,
        sleep_func: Sleep = time.sleep,
        now: datetime | None = None,
        upload_worker: R2UploadWorker | None = None,
    ) -> None:
        reference = now or datetime.utcnow()
        self.config = config
        self.sleep_func = sleep_func

        self.index_holder = IndexHolder(config.inputs, now=reference)
        self.render_queue = PriorityWorkQueue()
        self.ingest_queue = PriorityWorkQueue()
        self.want_board = ForecastWantBoard()
        self.upload_worker = upload_worker if upload_worker is not None else R2UploadWorker.disabled()
        output_ready = self.upload_worker.enqueue if self.upload_worker.enabled else None
        self.polling_scheduler = RadarScheduler(_polling_only_config(config), now=reference)
        self.download_worker = DownloadWorker(
            self.ingest_queue,
            self.render_queue,
            products=config.products,
            forecasts=config.forecasts,
            index_holder=self.index_holder,
            want_board=self.want_board,
            record_poll_result=self._record_poll_result,
        )
        self.render_worker = RenderWorker(self.render_queue, want_board=self.want_board, on_output_ready=output_ready)
        self.forecast_pool = ForecastGenPool(self.render_queue)
        self.mqtt = MqttWatcher(inputs=config.inputs, on_notification=self._on_mqtt_notification)
        self._stop_requested = Event()
        self._started = False

    # Producers ---------------------------------------------------------------

    def _on_mqtt_notification(self, notification: MqttNotification) -> None:
        """Runs on the paho network thread: enqueue only, never download."""

        if not notification.remotes:
            return
        self.ingest_queue.put_if_absent(
            INGEST_PRIORITY,
            MqttIngestTask(input=notification.input, remotes=notification.remotes),
        )

    def _record_poll_result(self, task: PollIngestTask, has_new: bool, now: datetime) -> None:
        self.polling_scheduler.record_source_result(task.input.source.id, has_new=has_new, now=now)

    def enqueue_due_polls(self, *, now: datetime) -> int:
        """Enqueue live polls for non-MQTT inputs that the scheduler marks due."""

        count = 0
        for input_config in self.polling_scheduler.due_inputs(now):
            task = PollIngestTask(input=input_config, reason="live_poll", limit_per_input=1)
            if self.ingest_queue.put_if_absent(INGEST_PRIORITY, task):
                count += 1
        return count

    def enqueue_all_inputs(self, *, reason: str) -> int:
        """Enqueue a full poll of every enabled input (startup/fallback/backfill)."""

        count = 0
        for input_config in self.config.inputs:
            if not input_config.enabled:
                continue
            task = PollIngestTask(
                input=input_config,
                reason=reason,
                limit_per_input=default_limit_per_input((input_config,)),
            )
            if self.ingest_queue.put_if_absent(INGEST_PRIORITY, task):
                count += 1
        return count

    # Forecast dispatch ---------------------------------------------------------

    def dispatch_forecast_generation(self) -> int:
        """Submit wanted forecast generations, but only when the render lane is idle."""

        if not self.render_queue.is_idle():
            return 0
        return self.forecast_pool.dispatch(self.want_board)

    # Lifecycle ---------------------------------------------------------------

    def reconcile_startup(self) -> None:
        """Rebuild pending work from the filesystem after a (re)start."""

        index = self.index_holder.get()
        rendered = enqueue_observed_render_jobs(self.render_queue, index, self.config.products)

        forecast_renders = 0
        for forecast in self.config.forecasts:
            if not forecast.enabled:
                continue
            for unit in forecast_store.discover_forecast_render_units(forecast):
                task = ForecastRenderTask(
                    forecast=forecast,
                    issue_timestamp=unit.issue_timestamp,
                    minute=unit.minute,
                    field_path=unit.field_path,
                    base=unit.base,
                )
                if self.render_queue.put_if_absent(forecast.priority, task):
                    forecast_renders += 1
        wants = update_forecast_wants(self.want_board, index, self.config.forecasts)
        LOGGER.info(
            "Startup reconciliation: %d observed renders, %d forecast renders, %d forecast wants",
            rendered,
            forecast_renders,
            wants,
        )

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        prune_all(
            inputs=self.config.inputs,
            products=self.config.products,
            forecasts=self.config.forecasts,
            now=datetime.utcnow(),
        )
        if self.upload_worker.enabled:
            self.upload_worker.start()
            self.upload_worker.enqueue_pending_outputs()
        self.download_worker.start()
        self.render_worker.start()
        if self.mqtt.default_policy() is not None:
            self.mqtt.start()
        else:
            LOGGER.info("No MQTT notification policy configured; relying on polling only")
        self.reconcile_startup()
        self.enqueue_all_inputs(reason="startup")

    def request_stop(self) -> None:
        self._stop_requested.set()

    def run_forever(
        self,
        *,
        sleep_seconds: float = 1.0,
        fallback_interval_seconds: int = 300,
        backfill_interval_seconds: int = 1800,
        mqtt_stale_seconds: int = 600,
        prune_interval_seconds: int = 60,
    ) -> None:
        self.start()
        now = datetime.utcnow()
        next_fallback_poll = now + timedelta(seconds=fallback_interval_seconds)
        next_backfill_poll = now + timedelta(seconds=backfill_interval_seconds)
        next_prune = now + timedelta(seconds=prune_interval_seconds)

        try:
            while not self._stop_requested.is_set():
                now = datetime.utcnow()
                self.enqueue_due_polls(now=now)

                if self.mqtt.is_stale(now=now, stale_after_seconds=mqtt_stale_seconds) and now >= next_fallback_poll:
                    LOGGER.info("MQTT disconnected or stale; enqueueing fallback polls")
                    self.enqueue_all_inputs(reason="mqtt_disconnected_or_stale")
                    next_fallback_poll = now + timedelta(seconds=fallback_interval_seconds)
                    next_backfill_poll = max(next_backfill_poll, now + timedelta(seconds=backfill_interval_seconds))
                elif now >= next_backfill_poll:
                    self.enqueue_all_inputs(reason="scheduled_backfill")
                    next_backfill_poll = now + timedelta(seconds=backfill_interval_seconds)

                self.dispatch_forecast_generation()

                if now >= next_prune:
                    prune_all(
                        inputs=self.config.inputs,
                        products=self.config.products,
                        forecasts=self.config.forecasts,
                        now=now,
                    )
                    next_prune = now + timedelta(seconds=prune_interval_seconds)

                self.sleep_func(sleep_seconds)
        finally:
            self.shutdown()

    def shutdown(self, *, join_timeout_seconds: float = 10.0) -> None:
        """Stop producers and workers; pending queue work is abandoned.

        Outputs are idempotent and rebuilt by startup reconciliation, so an
        in-memory queue lost here costs nothing but a little repeated work.
        """

        self._stop_requested.set()
        self.mqtt.stop()
        self.download_worker.stop()
        self.render_worker.stop()
        if self.download_worker.is_alive():
            self.download_worker.join(timeout=join_timeout_seconds)
        if self.render_worker.is_alive():
            LOGGER.info("Waiting for current render to finish before stopping R2 uploads")
            self.render_worker.join()
        if self.upload_worker.enabled and not self.upload_worker.drain(timeout=join_timeout_seconds):
            LOGGER.warning(
                "R2 upload drain timed out with %d pending PNGs; remaining files will retry on next start",
                self.upload_worker.pending_count(),
            )
        self.upload_worker.stop()
        if self.upload_worker.enabled and self.upload_worker.is_alive():
            self.upload_worker.join(timeout=join_timeout_seconds)
        self.forecast_pool.shutdown()
        LOGGER.info("Runtime shut down; pending queue work will be rebuilt on next start")


def run_forecasts_once(
    config: RadarServerConfig,
    *,
    now: datetime | None = None,
    on_output_ready: OutputReadyCallback | None = None,
) -> int:
    """Synchronously generate and render due forecasts (run-once CLI helper)."""

    from .forecast_generation import generate_for_task
    from .rendering.forecast import render_forecast_field

    index = LocalInputIndex.from_filesystem(config.inputs, now=now)
    rendered = 0
    for forecast in config.forecasts:
        task = build_forecast_gen_task(forecast, index)
        if task is None:
            continue
        fields_by_minute = generate_for_task(task)
        if not fields_by_minute:
            continue
        forecast_store.write_forecast_fields(forecast, task.issue_timestamp, fields_by_minute)
        for minute in sorted(fields_by_minute):
            base = forecast_store.forecast_base(forecast, task.issue_timestamp, minute)
            render_forecast_field(
                fields_by_minute[minute],
                forecast.output_dir,
                forecast.palette,
                base=base,
                minute=minute,
                variants=forecast.render_variants,
                optimize=forecast.optimize,
                on_output_ready=on_output_ready,
            )
            rendered += 1
    return rendered


def _polling_only_config(config: RadarServerConfig) -> RadarServerConfig:
    inputs = tuple(input_config for input_config in config.inputs if not _input_has_mqtt(input_config))
    source_ids = {input_config.source.id for input_config in inputs}
    sources = tuple(source for source in config.sources if source.id in source_ids)
    return RadarServerConfig(sources=sources, inputs=inputs, products=config.products, forecasts=config.forecasts)


def _input_has_mqtt(input_config: InputConfig) -> bool:
    source = input_config.source
    if not isinstance(source, OrdApiSource):
        return False
    return any(policy.kind == "mqtt" for policy in source.notifications)
