from datetime import datetime, timedelta
from pathlib import Path
from types import MethodType
from unittest.mock import Mock, patch

from new_version.scheduler import RadarScheduler


def make_scheduler() -> RadarScheduler:
    scheduler = RadarScheduler()
    # Prevent tests from touching the real backlog logic
    scheduler.run_cycle = MethodType(lambda self: False, scheduler)
    return scheduler


def test_enters_quick_mode_on_boundary():
    scheduler = make_scheduler()
    boundary = datetime(2025, 9, 26, 20, 0)
    scheduler.next_publish = boundary

    assert scheduler.quick_mode is False

    scheduler.step(boundary)

    assert scheduler.quick_mode is True
    # First step should trigger immediate attempt
    assert scheduler.quick_attempts == 1
    assert scheduler.quick_last_attempt == boundary


def test_quick_mode_exits_when_new_data_arrives():
    scheduler = make_scheduler()

    boundary = datetime(2025, 9, 26, 20, 0)
    scheduler.next_publish = boundary

    # Enter quick mode first step
    scheduler.step(boundary)
    assert scheduler.quick_mode

    # Simulate new data on next attempt
    def success_run_cycle(self):
        return True

    scheduler.run_cycle = MethodType(success_run_cycle, scheduler)
    # After interval elapsed, next attempt should succeed
    scheduler.step(boundary + timedelta(seconds=scheduler.config.timing.quick_check_interval + 1))

    assert scheduler.quick_mode is False
    assert scheduler.quick_attempts == 2
    assert scheduler.next_publish > boundary


def test_quick_mode_performs_retry_after_interval():
    scheduler = make_scheduler()
    boundary = datetime(2025, 9, 26, 20, 0)
    scheduler.next_publish = boundary
    attempts = []

    def tracking_run_cycle(self):
        attempts.append(1)
        return False

    scheduler.run_cycle = MethodType(tracking_run_cycle, scheduler)

    scheduler.step(boundary)
    assert scheduler.quick_mode
    assert scheduler.quick_attempts == 1
    assert len(attempts) == 1  # Immediate attempt on entry

    # No new attempt before interval
    scheduler.step(boundary + timedelta(seconds=1))
    assert len(attempts) == 1
    assert scheduler.quick_attempts == 1

    # After quick interval elapsed, another attempt fires
    scheduler.step(boundary + timedelta(seconds=scheduler.config.timing.quick_check_interval + 1))
    assert len(attempts) == 2
    assert scheduler.quick_attempts == 2


def test_run_cycle_updates_next_publish_when_latest_ready():
    scheduler = RadarScheduler()
    base_timestamp = datetime(2025, 9, 26, 20, 20)

    def fake_ensure(self):
        return False, base_timestamp, True

    scheduler._ensure_radar_backlog = MethodType(fake_ensure, scheduler)
    scheduler._download_forecast_tar = MethodType(lambda self: None, scheduler)
    scheduler._process_forecast = MethodType(lambda self, ts, path: None, scheduler)

    scheduler.next_publish = datetime(2025, 9, 26, 20, 15)
    scheduler.run_cycle()

    expected_next = scheduler._calculate_next_expected(base_timestamp)
    assert scheduler.next_publish == expected_next



