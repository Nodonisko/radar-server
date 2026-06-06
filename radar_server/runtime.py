"""Combined runtime: MQTT primary with polling/backfill fallback."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Callable

from .config import CONFIG, InputConfig, OrdApiSource, RadarServerConfig, SourceConfig
from .input_index import LocalInputIndex
from .mqtt_watcher import MqttWatcher
from .scheduler import RadarScheduler, SchedulerCycleResult

LOGGER = logging.getLogger(__name__)

Sleep = Callable[[float], None]


@dataclass(frozen=True)
class RuntimePollResult:
    reason: str
    result: SchedulerCycleResult


class RadarRuntime:
    """Run MQTT notifications and polling fallback against filesystem input snapshots."""

    def __init__(
        self,
        config: RadarServerConfig = CONFIG,
        *,
        input_index: LocalInputIndex | None = None,
        sleep_func: Sleep = time.sleep,
        now: datetime | None = None,
    ) -> None:
        reference = now or datetime.utcnow()
        self.config = config
        self.lock = RLock()
        self.input_index = input_index or LocalInputIndex.from_filesystem(config.inputs, now=reference)
        self.scheduler = RadarScheduler(config, input_index=self.input_index, sleep_func=sleep_func, now=reference)
        self.live_polling_scheduler = RadarScheduler(
            _polling_only_config(config),
            input_index=self.input_index,
            sleep_func=sleep_func,
            now=reference,
            index_inputs=config.inputs,
        )
        self.mqtt = MqttWatcher(inputs=config.inputs, products=config.products, input_index=self.input_index, lock=self.lock)
        self.sleep_func = sleep_func

    def start_mqtt(self) -> None:
        self.mqtt.start()

    def stop_mqtt(self) -> None:
        self.mqtt.stop()

    def run_polling_backfill(self, *, reason: str, now: datetime | None = None) -> RuntimePollResult:
        reference = now or datetime.utcnow()
        LOGGER.info("Running polling backfill (%s)", reason)
        with self.lock:
            result = self.scheduler.run_once(now=reference)
            self._refresh_shared_index(self.scheduler.input_index)
        return RuntimePollResult(reason=reason, result=result)

    def run_startup_backfill(self, *, now: datetime | None = None) -> RuntimePollResult:
        return self.run_polling_backfill(reason="startup", now=now)

    def run_live_polling_step(self, *, now: datetime | None = None) -> SchedulerCycleResult | None:
        reference = now or datetime.utcnow()
        with self.lock:
            result = self.live_polling_scheduler.step(reference, limit_per_input=1)
            if result is not None:
                self._refresh_shared_index(self.live_polling_scheduler.input_index)
            return result

    def run_forever(
        self,
        *,
        sleep_seconds: float = 1.0,
        fallback_interval_seconds: int = 300,
        backfill_interval_seconds: int = 1800,
        mqtt_stale_seconds: int = 600,
    ) -> None:
        self.start_mqtt()
        startup = self.run_startup_backfill()
        LOGGER.info(
            "Startup backfill finished: downloaded=%d rendered=%d",
            startup.result.downloaded_count,
            len(startup.result.rendered),
        )
        now = datetime.utcnow()
        next_fallback_poll = now + timedelta(seconds=fallback_interval_seconds)
        next_backfill_poll = now + timedelta(seconds=backfill_interval_seconds)

        try:
            while True:
                now = datetime.utcnow()
                live_poll = self.run_live_polling_step(now=now)
                if live_poll is not None:
                    LOGGER.info(
                        "Live polling finished: downloaded=%d rendered=%d",
                        live_poll.downloaded_count,
                        len(live_poll.rendered),
                    )

                if self.mqtt.is_stale(now=now, stale_after_seconds=mqtt_stale_seconds) and now >= next_fallback_poll:
                    reason = "mqtt_disconnected_or_stale"
                    result = self.run_polling_backfill(reason=reason, now=now)
                    LOGGER.info(
                        "Polling fallback finished: downloaded=%d rendered=%d",
                        result.result.downloaded_count,
                        len(result.result.rendered),
                    )
                    next_fallback_poll = now + timedelta(seconds=fallback_interval_seconds)
                    next_backfill_poll = max(next_backfill_poll, now + timedelta(seconds=backfill_interval_seconds))

                elif now >= next_backfill_poll:
                    result = self.run_polling_backfill(reason="scheduled_backfill", now=now)
                    LOGGER.info(
                        "Scheduled backfill finished: downloaded=%d rendered=%d",
                        result.result.downloaded_count,
                        len(result.result.rendered),
                    )
                    next_backfill_poll = now + timedelta(seconds=backfill_interval_seconds)

                self.sleep_func(sleep_seconds)
        finally:
            self.stop_mqtt()

    def _refresh_shared_index(self, input_index: LocalInputIndex) -> None:
        self.input_index = input_index
        self.scheduler.input_index = input_index
        self.live_polling_scheduler.input_index = input_index
        self.mqtt.input_index = input_index


def _polling_only_config(config: RadarServerConfig) -> RadarServerConfig:
    inputs = tuple(input_config for input_config in config.inputs if not _input_has_mqtt(input_config))
    source_ids = {input_config.source.id for input_config in inputs}
    sources = tuple(source for source in config.sources if source.id in source_ids)
    return RadarServerConfig(sources=sources, inputs=inputs, products=config.products)


def _input_has_mqtt(input_config: InputConfig) -> bool:
    source = input_config.source
    if not isinstance(source, OrdApiSource):
        return False
    return any(policy.kind == "mqtt" for policy in source.notifications)
