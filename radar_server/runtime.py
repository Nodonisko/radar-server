"""Combined runtime: MQTT primary with polling/backfill fallback."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Callable

from .config import CONFIG, RadarServerConfig
from .mqtt_watcher import MqttWatcher
from .registry import InputRegistry
from .scheduler import RadarScheduler, SchedulerCycleResult

LOGGER = logging.getLogger(__name__)

Sleep = Callable[[float], None]


@dataclass(frozen=True)
class RuntimePollResult:
    reason: str
    result: SchedulerCycleResult


class RadarRuntime:
    """Run MQTT notifications and polling fallback against one shared registry."""

    def __init__(
        self,
        config: RadarServerConfig = CONFIG,
        *,
        registry: InputRegistry | None = None,
        sleep_func: Sleep = time.sleep,
        now: datetime | None = None,
    ) -> None:
        reference = now or datetime.utcnow()
        self.config = config
        self.lock = RLock()
        self.registry = registry or InputRegistry.from_local_inputs(config.inputs, now=reference)
        self.scheduler = RadarScheduler(config, registry=self.registry, sleep_func=sleep_func, now=reference)
        self.mqtt = MqttWatcher(inputs=config.inputs, products=config.products, registry=self.registry, lock=self.lock)
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
        return RuntimePollResult(reason=reason, result=result)

    def run_startup_backfill(self, *, now: datetime | None = None) -> RuntimePollResult:
        return self.run_polling_backfill(reason="startup", now=now)

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
