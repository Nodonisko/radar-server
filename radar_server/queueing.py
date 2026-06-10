"""Priority work queue and self-contained task definitions for the runtime.

All heavy work (downloads, observed renders, forecast generation, forecast
frame renders) is expressed as one of the task dataclasses below. Tasks carry
the concrete inputs they need (file paths, remote URLs) so workers do not read
shared mutable state while executing.
"""

from __future__ import annotations

import itertools
import queue
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .config import ForecastProduct, InputConfig, ProductConfig
from .fetching import RemoteInputFile
from .render_jobs import RenderJob


@dataclass(frozen=True)
class ObservedRenderTask:
    """Render one observed product frame; ``job`` carries resolved input files."""

    job: RenderJob

    @property
    def product(self) -> ProductConfig:
        return self.job.product

    @property
    def timestamp(self) -> datetime:
        return self.job.timestamp

    @property
    def key(self) -> tuple:
        return ("observed_render", self.job.product.id, self.job.timestamp)


@dataclass(frozen=True)
class ForecastRenderTask:
    """Render one forecast lead frame from a stored field file."""

    forecast: ForecastProduct
    issue_timestamp: datetime
    minute: int
    field_path: Path
    base: str

    @property
    def key(self) -> tuple:
        return ("forecast_render", self.forecast.id, self.issue_timestamp, self.minute)


@dataclass(frozen=True)
class HistoryFrame:
    """One past timestamp of a parent product with its resolved input files."""

    timestamp: datetime
    paths: tuple[Path, ...]


@dataclass(frozen=True)
class ForecastGenTask:
    """Generate forecast fields for one forecast product at one issue time."""

    forecast: ForecastProduct
    issue_timestamp: datetime
    history: tuple[HistoryFrame, ...]

    @property
    def key(self) -> tuple:
        return ("forecast_gen", self.forecast.id, self.issue_timestamp)


@dataclass(frozen=True)
class PollIngestTask:
    """Discover and download new files for one input via its remote API."""

    input: InputConfig
    reason: str
    limit_per_input: int | None = None

    @property
    def key(self) -> tuple:
        return ("ingest_poll", self.input.id, self.limit_per_input)


@dataclass(frozen=True)
class MqttIngestTask:
    """Download concrete remote files announced by an MQTT notification."""

    input: InputConfig
    remotes: tuple[RemoteInputFile, ...]

    @property
    def key(self) -> tuple:
        return ("ingest_mqtt", self.input.id, tuple(remote.filename for remote in self.remotes))


class PriorityWorkQueue:
    """Thread-safe priority queue with key dedup and idle tracking.

    Lower priority numbers are served first; ties are FIFO via a monotonic
    sequence number (which also prevents Python from comparing task objects).
    A task's ``key`` stays reserved from enqueue until ``task_done``, so
    re-enqueueing identical pending or in-flight work is a no-op.
    """

    def __init__(self) -> None:
        self._queue: queue.PriorityQueue = queue.PriorityQueue()
        self._lock = threading.Lock()
        self._pending_keys: set[tuple] = set()
        self._seq = itertools.count()

    def put_if_absent(self, priority: int, task) -> bool:
        with self._lock:
            if task.key in self._pending_keys:
                return False
            self._pending_keys.add(task.key)
            self._queue.put((priority, next(self._seq), task))
        return True

    def get(self, timeout: float = 0.0):
        """Pop the highest-priority task; raises ``queue.Empty`` on timeout."""

        return self._queue.get(timeout=timeout)[2]

    def task_done(self, task) -> None:
        with self._lock:
            self._pending_keys.discard(task.key)
        self._queue.task_done()

    def is_idle(self) -> bool:
        """True when nothing is queued and nothing is in flight."""

        with self._lock:
            return not self._pending_keys

    def pending_count(self) -> int:
        return self._queue.qsize()

    def keys(self) -> frozenset:
        with self._lock:
            return frozenset(self._pending_keys)
