"""Polling scheduler: decides when inputs are due and tracks quick-poll state.

Two usage modes:

- ``run_once``/``step``/``run_forever``: the original synchronous cycle
  (sync -> prune -> reindex -> render), kept for the ``run-once``/``poll`` CLI
  commands and tests.
- ``due_inputs`` + ``record_source_result``: decision-only API used by the
  queue-based runtime, where downloads happen on the download worker. Decision
  state updates are guarded by a lock because decisions happen on the main
  loop while results are recorded from the download worker thread.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterable

from .config import CONFIG, InputConfig, RadarServerConfig, SourceConfig
from .fetching import InputSyncResult, sync_inputs
from .input_index import LocalInputIndex
from .pruning import prune_all
from .render_jobs import render_ready_jobs
from .rendering.pipeline import RenderResult

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchedulerCycleResult:
    synced: tuple[InputSyncResult, ...]
    rendered: tuple[RenderResult, ...]

    @property
    def downloaded_count(self) -> int:
        return sum(1 for result in self.synced for item in result.files if item.downloaded)

    @property
    def has_new_files(self) -> bool:
        return self.downloaded_count > 0


@dataclass
class SourceScheduleState:
    source: SourceConfig
    next_expected_publish: datetime
    next_baseline_poll: datetime
    quick_mode: bool = False
    quick_attempts: int = 0
    quick_last_attempt: datetime | None = None


SyncInputs = Callable[..., list[InputSyncResult]]
RenderReadyJobs = Callable[..., list[RenderResult]]
Sleep = Callable[[float], None]


class RadarScheduler:
    def __init__(
        self,
        config: RadarServerConfig = CONFIG,
        *,
        input_index: LocalInputIndex | None = None,
        sync_func: SyncInputs = sync_inputs,
        render_func: RenderReadyJobs = render_ready_jobs,
        sleep_func: Sleep = time.sleep,
        now: datetime | None = None,
        index_inputs: Iterable[InputConfig] | None = None,
    ) -> None:
        reference = now or datetime.utcnow()
        self.config = config
        self.index_inputs = tuple(index_inputs or config.inputs)
        self.input_index = input_index or LocalInputIndex.from_filesystem(self.index_inputs, now=reference)
        self.sync_func = sync_func
        self.render_func = render_func
        self.sleep_func = sleep_func
        self._state_lock = threading.Lock()
        self.source_states = {
            source.id: SourceScheduleState(
                source=source,
                next_expected_publish=_next_expected_publish(reference, source.polling.expected_period_seconds),
                next_baseline_poll=reference,
            )
            for source in config.sources
        }

    # Decision-only API (queue-based runtime) --------------------------------

    def due_inputs(self, now: datetime) -> tuple[InputConfig, ...]:
        """Inputs that should be polled now; marks the poll attempt."""

        with self._state_lock:
            due_ids = self._due_source_ids(now)
        if not due_ids:
            return ()
        return tuple(
            input_config
            for input_config in self.config.inputs
            if input_config.source.id in due_ids and input_config.enabled
        )

    def record_source_result(self, source_id: str, *, has_new: bool, now: datetime) -> None:
        """Record the outcome of a poll for one source (any thread)."""

        with self._state_lock:
            state = self.source_states.get(source_id)
            if state is None:
                return
            policy = state.source.polling

            if state.quick_mode:
                if has_new:
                    LOGGER.info("%s: new file found, leaving quick polling", state.source.id)
                    self._exit_quick_mode(state, now)
                elif state.quick_attempts >= policy.quick_check_limit:
                    LOGGER.warning(
                        "%s: quick polling exhausted after %d attempts",
                        state.source.id,
                        state.quick_attempts,
                    )
                    self._exit_quick_mode(state, now)
                return

            if has_new:
                LOGGER.info("%s: new file found during baseline poll", state.source.id)
                state.next_expected_publish = _next_expected_publish(now, policy.expected_period_seconds)

    def _exit_quick_mode(self, state: SourceScheduleState, now: datetime) -> None:
        policy = state.source.polling
        state.quick_mode = False
        state.quick_attempts = 0
        state.quick_last_attempt = None
        state.next_expected_publish = _next_expected_publish(now, policy.expected_period_seconds)
        state.next_baseline_poll = now + timedelta(seconds=policy.baseline_interval_seconds)

    # Synchronous cycle API (run-once / poll CLI) ----------------------------

    def run_once(
        self,
        inputs: Iterable[InputConfig] | None = None,
        *,
        now: datetime | None = None,
        limit_per_input: int | None = None,
    ) -> SchedulerCycleResult:
        input_configs = tuple(inputs or self.config.inputs)
        reference = now or datetime.utcnow()
        limit = limit_per_input if limit_per_input is not None else default_limit_per_input(input_configs)
        synced = tuple(self.sync_func(input_configs, now=reference, limit_per_input=limit))
        prune_all(
            inputs=self.config.inputs,
            products=self.config.products,
            forecasts=self.config.forecasts,
            now=reference,
        )
        self.input_index = LocalInputIndex.from_filesystem(self.index_inputs, now=reference)
        rendered = tuple(self.render_func(self.input_index, self.config.products))
        return SchedulerCycleResult(synced=synced, rendered=rendered)

    def step(self, now: datetime | None = None, *, limit_per_input: int | None = None) -> SchedulerCycleResult | None:
        reference = now or datetime.utcnow()
        with self._state_lock:
            due_source_ids = self._due_source_ids(reference)
        if not due_source_ids:
            return None

        due_inputs = tuple(
            input_config
            for input_config in self.config.inputs
            if input_config.source.id in due_source_ids and input_config.enabled
        )
        result = self.run_once(due_inputs, now=reference, limit_per_input=limit_per_input)
        source_has_new = _source_downloads(result)
        for source_id in due_source_ids:
            self.record_source_result(source_id, has_new=source_has_new.get(source_id, False), now=reference)
        return result

    def run_forever(self, *, sleep_seconds: float = 1.0) -> None:
        while True:
            self.step(datetime.utcnow())
            self.sleep_func(sleep_seconds)

    def _due_source_ids(self, now: datetime) -> tuple[str, ...]:
        """Decide due sources and mark attempts. Caller must hold the lock."""

        due: list[str] = []
        for source_id, state in self.source_states.items():
            policy = state.source.polling
            if not state.quick_mode and now >= state.next_expected_publish:
                state.quick_mode = True
                state.quick_attempts = 1
                state.quick_last_attempt = now
                LOGGER.info(
                    "%s: entering quick polling at expected boundary %s",
                    state.source.id,
                    state.next_expected_publish.strftime("%Y-%m-%d %H:%M:%S"),
                )
                LOGGER.info("%s: quick polling attempt 1/%d", state.source.id, policy.quick_check_limit)
                due.append(source_id)
                continue

            if state.quick_mode:
                elapsed = (
                    (now - state.quick_last_attempt).total_seconds()
                    if state.quick_last_attempt is not None
                    else None
                )
                if elapsed is None or elapsed >= policy.quick_check_interval_seconds:
                    state.quick_attempts += 1
                    state.quick_last_attempt = now
                    LOGGER.info(
                        "%s: quick polling attempt %d/%d",
                        state.source.id,
                        state.quick_attempts,
                        policy.quick_check_limit,
                    )
                    due.append(source_id)
                continue

            if now >= state.next_baseline_poll:
                state.next_baseline_poll = now + timedelta(seconds=policy.baseline_interval_seconds)
                LOGGER.info("%s: baseline poll due", state.source.id)
                due.append(source_id)
        return tuple(due)


def default_limit_per_input(inputs: tuple[InputConfig, ...]) -> int | None:
    limits: list[int] = []
    for input_config in inputs:
        keep_for = input_config.retention.keep_for_seconds
        if keep_for is None:
            continue
        period = max(1, input_config.source.polling.expected_period_seconds)
        limits.append(max(1, int(keep_for // period) + 2))
    if not limits:
        return None
    return max(limits)


def _source_downloads(result: SchedulerCycleResult) -> dict[str, bool]:
    downloads: dict[str, bool] = {}
    for sync_result in result.synced:
        downloads.setdefault(sync_result.input.source.id, False)
        if any(item.downloaded for item in sync_result.files):
            downloads[sync_result.input.source.id] = True
    return downloads


def _next_expected_publish(reference: datetime, period_seconds: int) -> datetime:
    period = max(1, period_seconds)
    day_start = reference.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((reference - day_start).total_seconds())
    next_elapsed = (elapsed // period + 1) * period
    return day_start + timedelta(seconds=next_elapsed)
