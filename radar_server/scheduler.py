"""Runtime scheduler that connects fetching, registry updates, and rendering."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterable

from .config import CONFIG, InputConfig, RadarServerConfig, SourceConfig
from .fetching import InputSyncResult, sync_inputs
from .registry import InputRegistry
from .render_jobs import render_ready_jobs
from .rendering.pipeline import RenderResult


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
        registry: InputRegistry | None = None,
        sync_func: SyncInputs = sync_inputs,
        render_func: RenderReadyJobs = render_ready_jobs,
        sleep_func: Sleep = time.sleep,
        now: datetime | None = None,
    ) -> None:
        reference = now or datetime.utcnow()
        self.config = config
        self.registry = registry or InputRegistry.from_local_inputs(config.inputs, now=reference)
        self.sync_func = sync_func
        self.render_func = render_func
        self.sleep_func = sleep_func
        self.source_states = {
            source.id: SourceScheduleState(
                source=source,
                next_expected_publish=_next_expected_publish(reference, source.polling.expected_period_seconds),
                next_baseline_poll=reference,
            )
            for source in config.sources
        }

    def run_once(
        self,
        inputs: Iterable[InputConfig] | None = None,
        *,
        now: datetime | None = None,
        limit_per_input: int | None = None,
    ) -> SchedulerCycleResult:
        input_configs = tuple(inputs or self.config.inputs)
        reference = now or datetime.utcnow()
        limit = limit_per_input if limit_per_input is not None else _default_limit_per_input(input_configs)
        synced = tuple(self.sync_func(input_configs, now=reference, limit_per_input=limit))
        self.registry.add_sync_results(synced)
        self.registry.prune(self.config.inputs, now=reference)
        rendered = tuple(self.render_func(self.registry, self.config.products))
        return SchedulerCycleResult(synced=synced, rendered=rendered)

    def step(self, now: datetime | None = None) -> SchedulerCycleResult | None:
        reference = now or datetime.utcnow()
        due_source_ids = self._due_source_ids(reference)
        if not due_source_ids:
            return None

        due_inputs = tuple(
            input_config
            for input_config in self.config.inputs
            if input_config.source.id in due_source_ids and input_config.enabled
        )
        result = self.run_once(due_inputs, now=reference)
        self._update_due_sources(due_source_ids, result, reference)
        return result

    def run_forever(self, *, sleep_seconds: float = 1.0) -> None:
        while True:
            self.step(datetime.utcnow())
            self.sleep_func(sleep_seconds)

    def _due_source_ids(self, now: datetime) -> tuple[str, ...]:
        due: list[str] = []
        for source_id, state in self.source_states.items():
            policy = state.source.polling
            if not state.quick_mode and now >= state.next_expected_publish:
                state.quick_mode = True
                state.quick_attempts = 0
                state.quick_last_attempt = None
                due.append(source_id)
                continue

            if state.quick_mode:
                if state.quick_last_attempt is None:
                    due.append(source_id)
                    continue
                elapsed = (now - state.quick_last_attempt).total_seconds()
                if elapsed >= policy.quick_check_interval_seconds:
                    due.append(source_id)
                continue

            if now >= state.next_baseline_poll:
                due.append(source_id)
        return tuple(due)

    def _update_due_sources(
        self,
        source_ids: Iterable[str],
        result: SchedulerCycleResult,
        now: datetime,
    ) -> None:
        source_has_new = _source_downloads(result)
        for source_id in source_ids:
            state = self.source_states[source_id]
            policy = state.source.polling
            has_new = source_has_new.get(source_id, False)

            if state.quick_mode:
                state.quick_attempts += 1
                state.quick_last_attempt = now
                if has_new or state.quick_attempts >= policy.quick_check_limit:
                    state.quick_mode = False
                    state.quick_attempts = 0
                    state.quick_last_attempt = None
                    state.next_expected_publish = _next_expected_publish(now, policy.expected_period_seconds)
                    state.next_baseline_poll = now + timedelta(seconds=policy.baseline_interval_seconds)
                continue

            state.next_baseline_poll = now + timedelta(seconds=policy.baseline_interval_seconds)
            if has_new:
                state.next_expected_publish = _next_expected_publish(now, policy.expected_period_seconds)


def _default_limit_per_input(inputs: tuple[InputConfig, ...]) -> int | None:
    limits: list[int] = []
    for input_config in inputs:
        expire_after = input_config.availability.expire_after_seconds
        if expire_after is None:
            continue
        period = max(1, input_config.source.polling.expected_period_seconds)
        limits.append(max(1, int(expire_after // period) + 2))
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
