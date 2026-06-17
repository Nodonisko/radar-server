"""Worker threads: ingest/download, the sequential render lane, and the
forecast generation pool.

Roles (see runtime.py for wiring):

- ``DownloadWorker`` (1 thread) drains the ingest queue: it downloads files,
  refreshes the shared input index, enqueues observed render tasks by product
  priority, and records the latest wanted forecast generation per product.
- ``RenderWorker`` threads drain the render priority queue in parallel (see
  ``RENDER_WORKER_COUNT``); strict numeric priority means observed frames
  always beat forecast frames.
- ``ForecastGenPool`` runs pysteps motion/extrapolation up to N wide; it is
  dispatched by the runtime only when the render lane is idle, writes the
  generated fields to the on-disk store, then enqueues forecast render tasks.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Empty
from typing import Callable, Iterable

from . import forecast_store
from .config import ForecastProduct, InputConfig, ProductConfig
from .fetching import FetchError, LocalInputFile, download_remote_file, sync_input
from .input_index import LocalInputIndex
from .queueing import (
    ForecastGenTask,
    ForecastRenderTask,
    HistoryFrame,
    MqttIngestTask,
    ObservedRenderTask,
    PollIngestTask,
    PriorityWorkQueue,
)
from .render_jobs import bounds_tuple, outputs_exist, render_job, resolve_render_jobs
from .rendering.core import RadarField
from .rendering.forecast import render_forecast_field
from .rendering.pipeline import OutputReadyCallback

LOGGER = logging.getLogger(__name__)

RENDER_WORKER_COUNT = 4
FORECAST_GEN_WORKER_COUNT = 6


class IndexHolder:
    """Thread-safe holder for the immutable filesystem input index."""

    def __init__(
        self,
        inputs: Iterable[InputConfig],
        *,
        now: datetime | None = None,
        index: LocalInputIndex | None = None,
    ) -> None:
        self._inputs = tuple(inputs)
        self._lock = threading.Lock()
        self._index = index or LocalInputIndex.from_filesystem(self._inputs, now=now)

    def get(self) -> LocalInputIndex:
        with self._lock:
            return self._index

    def rebuild(self, *, now: datetime | None = None) -> LocalInputIndex:
        index = LocalInputIndex.from_filesystem(self._inputs, now=now)
        with self._lock:
            self._index = index
        return index


class ForecastWantBoard:
    """Latest wanted forecast generation per forecast product.

    Setting a newer issue time overwrites the older one, which is the
    coalescing rule: only the latest forecast per product is worth generating.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._wanted: dict[str, ForecastGenTask] = {}
        self._latest_issue: dict[str, datetime] = {}

    def set(self, task: ForecastGenTask) -> None:
        with self._lock:
            self._wanted[task.forecast.id] = task
            self._latest_issue[task.forecast.id] = task.issue_timestamp

    def pop_next(self, *, exclude: Iterable[str] = ()) -> ForecastGenTask | None:
        excluded = set(exclude)
        with self._lock:
            candidates = [task for fid, task in self._wanted.items() if fid not in excluded]
            if not candidates:
                return None
            task = min(candidates, key=lambda item: item.forecast.priority)
            return self._wanted.pop(task.forecast.id)

    def snapshot(self) -> dict[str, ForecastGenTask]:
        with self._lock:
            return dict(self._wanted)

    def latest_issue(self, forecast_id: str) -> datetime | None:
        with self._lock:
            return self._latest_issue.get(forecast_id)

    def has_newer_issue(self, forecast_id: str, issue_timestamp: datetime) -> bool:
        latest = self.latest_issue(forecast_id)
        return latest is not None and latest > issue_timestamp

    def __len__(self) -> int:
        with self._lock:
            return len(self._wanted)


class _QueueWorker(threading.Thread):
    """Base loop: pop one task, execute with failure isolation, repeat."""

    def __init__(self, work_queue: PriorityWorkQueue, *, name: str, poll_interval: float = 0.5) -> None:
        super().__init__(name=name, daemon=True)
        self._work_queue = work_queue
        self._poll_interval = poll_interval
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.process_one(timeout=self._poll_interval)

    def process_one(self, *, timeout: float = 0.0) -> bool:
        """Process at most one task; returns False when the queue was empty."""

        try:
            task = self._work_queue.get(timeout=timeout)
        except Empty:
            return False
        try:
            self._execute(task)
        except Exception:
            LOGGER.exception("%s: task failed: %r", self.name, task)
        finally:
            self._work_queue.task_done(task)
        return True

    def _execute(self, task) -> None:  # noqa: ANN001
        raise NotImplementedError


class RenderWorker(_QueueWorker):
    """Render lane worker ordered by numeric priority."""

    def __init__(
        self,
        render_queue: PriorityWorkQueue,
        *,
        want_board: ForecastWantBoard | None = None,
        on_output_ready: OutputReadyCallback | None = None,
        poll_interval: float = 0.5,
        name: str = "render-worker",
    ) -> None:
        super().__init__(render_queue, name=name, poll_interval=poll_interval)
        self._want_board = want_board
        self._on_output_ready = on_output_ready

    def _execute(self, task) -> None:  # noqa: ANN001
        if isinstance(task, ObservedRenderTask):
            self._render_observed(task)
        elif isinstance(task, ForecastRenderTask):
            self._render_forecast(task)
        else:
            LOGGER.error("render-worker: unknown task type %r", task)

    def _render_observed(self, task: ObservedRenderTask) -> None:
        job = task.job
        if outputs_exist(job):
            LOGGER.debug("Skipping render for %s at %s: outputs already exist", job.product.id, job.timestamp)
            return
        paths = [item.path for item in job.files]
        if not paths:
            LOGGER.warning("Skipping render for %s at %s: no input files", job.product.id, job.timestamp)
            return
        missing = [path for path in paths if not path.exists()]
        if missing:
            LOGGER.warning(
                "Skipping render for %s at %s: missing input files %s (pruned?)",
                job.product.id,
                job.timestamp,
                [path.name for path in missing],
            )
            return
        render_job(job, skip_existing=True, on_output_ready=self._on_output_ready)

    def _render_forecast(self, task: ForecastRenderTask) -> None:
        forecast = task.forecast
        if self._want_board is not None and self._want_board.has_newer_issue(forecast.id, task.issue_timestamp):
            LOGGER.info("Skipping forecast frame %s: newer forecast issue is wanted", task.base)
            return
        if all(path.exists() for path in forecast_store.frame_output_paths(forecast, task.base)):
            LOGGER.debug("Skipping forecast frame %s: outputs already exist", task.base)
            return
        if not task.field_path.exists():
            LOGGER.info("Skipping forecast frame %s: field file missing (superseded or pruned)", task.base)
            return
        try:
            stored = forecast_store.load_field(task.field_path)
        except Exception:
            LOGGER.warning("Skipping forecast frame %s: unreadable field file %s", task.base, task.field_path)
            return
        render_forecast_field(
            stored.field,
            forecast.output_dir,
            forecast.palette,
            base=task.base,
            minute=task.minute,
            variants=forecast.render_variants,
            optimize=forecast.optimize,
            bounds=bounds_tuple(forecast.geo_bounds),
            on_output_ready=self._on_output_ready,
        )


RecordPollResult = Callable[[PollIngestTask, bool, datetime], None]


class DownloadWorker(_QueueWorker):
    """Single download/ingest thread: network I/O never blocks MQTT or renders."""

    def __init__(
        self,
        ingest_queue: PriorityWorkQueue,
        render_queue: PriorityWorkQueue,
        *,
        products: Iterable[ProductConfig],
        forecasts: Iterable[ForecastProduct] = (),
        index_holder: IndexHolder,
        want_board: ForecastWantBoard | None = None,
        record_poll_result: RecordPollResult | None = None,
        poll_interval: float = 0.5,
    ) -> None:
        super().__init__(ingest_queue, name="download-worker", poll_interval=poll_interval)
        self._render_queue = render_queue
        self._products = tuple(products)
        self._forecasts = tuple(forecasts)
        self._index_holder = index_holder
        self._want_board = want_board if want_board is not None else ForecastWantBoard()
        self.record_poll_result = record_poll_result
        self.sync_func = sync_input
        self.download_func = download_remote_file
        self.now_func: Callable[[], datetime] = datetime.utcnow

    def _execute(self, task) -> None:  # noqa: ANN001
        now = self.now_func()
        if isinstance(task, PollIngestTask):
            files = self._sync(task, now)
            has_new = any(item.downloaded for item in files)
            if self.record_poll_result is not None:
                self.record_poll_result(task, has_new, now)
        elif isinstance(task, MqttIngestTask):
            files = self._download_remotes(task)
            has_new = any(item.downloaded for item in files)
        else:
            LOGGER.error("download-worker: unknown task type %r", task)
            return
        if has_new:
            self.refresh_and_enqueue(now=now)

    def _sync(self, task: PollIngestTask, now: datetime) -> list[LocalInputFile]:
        try:
            return self.sync_func(task.input, now=now, limit=task.limit_per_input)
        except FetchError as exc:
            LOGGER.warning("Input %s ingest failed (%s): %s", task.input.id, task.reason, exc)
            return []

    def _download_remotes(self, task: MqttIngestTask) -> list[LocalInputFile]:
        files: list[LocalInputFile] = []
        for remote in task.remotes:
            try:
                files.append(self.download_func(remote))
            except Exception as exc:
                LOGGER.warning("MQTT-announced download failed for %s: %s", remote.url, exc)
        return files

    def refresh_and_enqueue(self, *, now: datetime | None = None) -> LocalInputIndex:
        """Rebuild the index, enqueue missing observed renders, refresh wants."""

        index = self._index_holder.rebuild(now=now)
        enqueued = enqueue_observed_render_jobs(self._render_queue, index, self._products)
        wants = update_forecast_wants(self._want_board, index, self._forecasts)
        level = logging.INFO if enqueued or wants else logging.DEBUG
        LOGGER.log(level, "Ingest refresh: %d render tasks enqueued, %d forecast wants updated", enqueued, wants)
        return index


def enqueue_observed_render_jobs(
    render_queue: PriorityWorkQueue,
    index: LocalInputIndex,
    products: Iterable[ProductConfig],
) -> int:
    """Enqueue all missing observed renders, newest first within each priority."""

    jobs = resolve_render_jobs(index, tuple(products))
    jobs.sort(key=lambda job: job.timestamp, reverse=True)
    jobs.sort(key=lambda job: job.product.priority)
    count = 0
    for job in jobs:
        if render_queue.put_if_absent(job.product.priority, ObservedRenderTask(job=job)):
            count += 1
    return count


def build_forecast_gen_task(forecast: ForecastProduct, index: LocalInputIndex) -> ForecastGenTask | None:
    """Resolve the latest generatable issue for one forecast product, or None."""

    if not forecast.enabled or not forecast.minutes:
        return None
    parent = forecast.parent
    timestamps = sorted(index.ready_timestamps(parent))
    if len(timestamps) < forecast.history_frames:
        LOGGER.debug(
            "Forecast %s not ready: need %d history frames, have %d",
            forecast.id,
            forecast.history_frames,
            len(timestamps),
        )
        return None
    issue_timestamp = timestamps[-1]
    if all(path.exists() for path in forecast_store.expected_forecast_paths(forecast, issue_timestamp)):
        return None

    frames: list[HistoryFrame] = []
    for timestamp in timestamps[-forecast.history_frames :]:
        paths = tuple(
            item.path
            for input_config in parent.inputs
            for item in index.files_for(input_config, timestamp)
        )
        if not paths:
            LOGGER.info("Forecast %s skipped: missing input files for %s", forecast.id, timestamp)
            return None
        frames.append(HistoryFrame(timestamp=timestamp, paths=paths))
    return ForecastGenTask(forecast=forecast, issue_timestamp=issue_timestamp, history=tuple(frames))


def update_forecast_wants(
    want_board: ForecastWantBoard,
    index: LocalInputIndex,
    forecasts: Iterable[ForecastProduct],
) -> int:
    count = 0
    for forecast in forecasts:
        task = build_forecast_gen_task(forecast, index)
        if task is not None:
            want_board.set(task)
            count += 1
    return count


GenerateFunc = Callable[[ForecastGenTask], "dict[int, RadarField]"]


def _default_generate(task: ForecastGenTask) -> dict[int, RadarField]:
    from .forecast_generation import generate_for_task

    return generate_for_task(task)


class ForecastGenPool:
    """Bounded pool for forecast generation (motion + extrapolation).

    pysteps motion only scales to ~4 threads, so running a couple of
    generations in parallel fills otherwise idle cores; the runtime gates
    dispatch on the render lane being idle so generation never competes with
    observed renders.
    """

    def __init__(
        self,
        render_queue: PriorityWorkQueue,
        *,
        max_workers: int = FORECAST_GEN_WORKER_COUNT,
        generate_func: GenerateFunc | None = None,
        executor=None,  # noqa: ANN001 - test seam (inline executor)
    ) -> None:
        self._render_queue = render_queue
        self._max_workers = max_workers
        self._executor = executor or ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="forecast-gen")
        self._lock = threading.Lock()
        self._in_flight: set[str] = set()
        self.generate_func: GenerateFunc = generate_func or _default_generate

    def in_flight_count(self) -> int:
        with self._lock:
            return len(self._in_flight)

    def dispatch(self, want_board: ForecastWantBoard) -> int:
        """Submit wanted generations up to the worker limit; returns count."""

        submitted = 0
        while True:
            with self._lock:
                if len(self._in_flight) >= self._max_workers:
                    break
                task = want_board.pop_next(exclude=self._in_flight)
                if task is None:
                    break
                self._in_flight.add(task.forecast.id)
            self._executor.submit(self._run, task, want_board)
            submitted += 1
        return submitted

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _run(self, task: ForecastGenTask, want_board: ForecastWantBoard) -> None:
        try:
            self._process(task, want_board)
        except Exception:
            LOGGER.exception("Forecast generation failed for %s at %s", task.forecast.id, task.issue_timestamp)
        finally:
            with self._lock:
                self._in_flight.discard(task.forecast.id)

    def _process(self, task: ForecastGenTask, want_board: ForecastWantBoard) -> None:
        forecast = task.forecast
        issue_timestamp = task.issue_timestamp
        if want_board.has_newer_issue(forecast.id, issue_timestamp):
            LOGGER.info("Skipping forecast generation for %s at %s: superseded", forecast.id, issue_timestamp)
            return
        if all(path.exists() for path in forecast_store.expected_forecast_paths(forecast, issue_timestamp)):
            LOGGER.debug("Forecast %s at %s already rendered; skipping generation", forecast.id, issue_timestamp)
            return

        existing = forecast_store.existing_field_paths(forecast, issue_timestamp)
        if set(existing) == set(forecast.minutes):
            # Fields survived a restart/crash before rendering: reuse them.
            field_paths = existing
        else:
            LOGGER.info("Starting forecast generation for %s at %s", forecast.id, issue_timestamp)
            fields_by_minute = self.generate_func(task)
            if not fields_by_minute:
                return
            if want_board.has_newer_issue(forecast.id, issue_timestamp):
                LOGGER.info("Discarding generated forecast for %s at %s: superseded", forecast.id, issue_timestamp)
                return
            field_paths = forecast_store.write_forecast_fields(forecast, issue_timestamp, fields_by_minute)

        if want_board.has_newer_issue(forecast.id, issue_timestamp):
            LOGGER.info("Skipping forecast render enqueue for %s at %s: superseded", forecast.id, issue_timestamp)
            return
        self._enqueue_renders(forecast, issue_timestamp, field_paths)

    def _enqueue_renders(self, forecast: ForecastProduct, issue_timestamp: datetime, field_paths) -> None:  # noqa: ANN001
        for minute in sorted(field_paths):
            base = forecast_store.forecast_base(forecast, issue_timestamp, minute)
            self._render_queue.put_if_absent(
                forecast.priority,
                ForecastRenderTask(
                    forecast=forecast,
                    issue_timestamp=issue_timestamp,
                    minute=minute,
                    field_path=field_paths[minute],
                    base=base,
                ),
            )
