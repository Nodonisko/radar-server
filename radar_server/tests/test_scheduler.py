from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from radar_server.config import (
    GeoBounds,
    GeoCenter,
    RadarServerConfig,
    SmartPollingPolicy,
    ProductConfig,
    chmi_current,
    cz_maxz,
    timestamped_base,
)
from radar_server.fetching import InputSyncResult, LocalInputFile, RemoteInputFile
from radar_server.scheduler import RadarScheduler


def _input(tmp_path: Path):
    source = replace(
        chmi_current,
        polling=SmartPollingPolicy(
            expected_period_seconds=300,
            baseline_interval_seconds=300,
            quick_check_interval_seconds=3,
            quick_check_limit=2,
        ),
    )
    return replace(cz_maxz, source=source, local_dir=tmp_path / "input")


def _product(tmp_path: Path, input_config) -> ProductConfig:
    return ProductConfig(
        id="test",
        label="Test",
        inputs=(input_config,),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(0, 0, 0, 0),
        center=GeoCenter(0.0, 0.0),
        publish_delay_seconds=260,
        base_name=timestamped_base("radar_test"),
    )


def _config(tmp_path: Path) -> tuple[RadarServerConfig, object]:
    input_config = _input(tmp_path)
    product = _product(tmp_path, input_config)
    return RadarServerConfig(sources=(input_config.source,), inputs=(input_config,), products=(product,)), input_config


def _sync_result(input_config, timestamp: datetime, *, downloaded: bool) -> InputSyncResult:
    path = input_config.local_dir / f"T_PABV23_C_OKPR_{timestamp:%Y%m%d%H%M}00.hdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"hdf")
    remote = RemoteInputFile(
        input=input_config,
        timestamp=timestamp,
        url=f"https://example.test/{path.name}",
        filename=path.name,
        metadata={},
    )
    local = LocalInputFile(
        input=input_config,
        timestamp=timestamp,
        path=path,
        remote=remote,
        downloaded=downloaded,
    )
    return InputSyncResult(input=input_config, files=(local,))


def test_run_once_syncs_filesystem_index_and_renders(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    now = datetime(2026, 6, 5, 21, 5)
    sync_calls = []
    render_calls = []

    def fake_sync(inputs, *, now=None, limit_per_input=None):
        sync_calls.append((tuple(inputs), now, limit_per_input))
        return [_sync_result(input_config, now, downloaded=True)]

    def fake_render(input_index, products):
        render_calls.append((input_index, tuple(products)))
        assert input_index.timestamps_for(input_config) == {now}
        return []

    scheduler = RadarScheduler(config, sync_func=fake_sync, render_func=fake_render, now=now)
    result = scheduler.run_once(now=now)

    assert result.downloaded_count == 1
    assert sync_calls[0][0] == (input_config,)
    assert sync_calls[0][2] == 26
    assert render_calls[0][1] == config.products


def test_step_runs_initial_baseline_poll(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    now = datetime(2026, 6, 5, 20, 58)
    sync_count = 0

    def fake_sync(inputs, *, now=None, limit_per_input=None):  # noqa: ARG001
        nonlocal sync_count
        sync_count += 1
        return [_sync_result(input_config, now, downloaded=False)]

    scheduler = RadarScheduler(config, sync_func=fake_sync, render_func=lambda input_index, products: [], now=now)

    assert scheduler.step(now) is not None
    assert sync_count == 1
    state = scheduler.source_states[input_config.source.id]
    assert state.next_baseline_poll == now + timedelta(seconds=300)


def test_step_enters_quick_mode_at_expected_boundary_and_respects_interval(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    start = datetime(2026, 6, 5, 20, 58)
    boundary = datetime(2026, 6, 5, 21, 0)
    sync_count = 0

    def fake_sync(inputs, *, now=None, limit_per_input=None):  # noqa: ARG001
        nonlocal sync_count
        sync_count += 1
        return [_sync_result(input_config, now, downloaded=False)]

    scheduler = RadarScheduler(config, sync_func=fake_sync, render_func=lambda input_index, products: [], now=start)

    assert scheduler.step(boundary) is not None
    state = scheduler.source_states[input_config.source.id]
    assert state.quick_mode is True
    assert state.quick_attempts == 1

    assert scheduler.step(boundary + timedelta(seconds=1)) is None
    assert sync_count == 1

    assert scheduler.step(boundary + timedelta(seconds=3)) is not None
    assert sync_count == 2


def test_quick_mode_exits_when_new_file_arrives(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    start = datetime(2026, 6, 5, 20, 58)
    boundary = datetime(2026, 6, 5, 21, 0)
    downloads = [False, True]

    def fake_sync(inputs, *, now=None, limit_per_input=None):  # noqa: ARG001
        return [_sync_result(input_config, now, downloaded=downloads.pop(0))]

    scheduler = RadarScheduler(config, sync_func=fake_sync, render_func=lambda input_index, products: [], now=start)
    scheduler.step(boundary)
    result = scheduler.step(boundary + timedelta(seconds=3))

    assert result is not None
    assert result.has_new_files is True
    state = scheduler.source_states[input_config.source.id]
    assert state.quick_mode is False
    assert state.quick_attempts == 0
    assert state.next_expected_publish == boundary + timedelta(minutes=5)
