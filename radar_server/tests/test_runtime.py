from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

from radar_server.config import ProductConfig, RadarServerConfig, chmi_current, cz_maxz, timestamped_base
from radar_server.fetching import InputSyncResult, LocalInputFile, RemoteInputFile
from radar_server.runtime import RadarRuntime


def _config(tmp_path: Path) -> tuple[RadarServerConfig, object]:
    input_config = replace(cz_maxz, local_dir=tmp_path / "input")
    product = ProductConfig(
        id="test",
        label="Test",
        inputs=(input_config,),
        output_dir=tmp_path / "out",
        geo_bounds=None,
        base_name=timestamped_base("radar_test"),
    )
    return RadarServerConfig(sources=(chmi_current,), inputs=(input_config,), products=(product,)), input_config


def test_polling_backfill_uses_fresh_filesystem_index(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    now = datetime(2026, 6, 5, 21, 5)
    calls = []

    def fake_sync(inputs, *, now=None, limit_per_input=None):
        path = input_config.local_dir / f"T_PABV23_C_OKPR_{now:%Y%m%d%H%M}00.hdf"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"hdf")
        remote = RemoteInputFile(input_config, now, f"https://example.test/{path.name}", path.name, {})
        local = LocalInputFile(input_config, now, path, remote, downloaded=True)
        return [InputSyncResult(input=input_config, files=(local,))]

    def fake_render(input_index, products):
        calls.append((input_index, tuple(products)))
        assert input_index.timestamps_for(input_config) == {now}
        return []

    runtime = RadarRuntime(config, now=now)
    runtime.scheduler.sync_func = fake_sync
    runtime.scheduler.render_func = fake_render
    result = runtime.run_polling_backfill(reason="test", now=now)

    assert result.reason == "test"
    assert result.result.downloaded_count == 1
    assert calls[0][0] is runtime.input_index


def test_run_forever_runs_startup_backfill_before_loop(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    now = datetime(2026, 6, 5, 21, 5)
    calls = []

    def fake_sync(inputs, *, now=None, limit_per_input=None):
        path = input_config.local_dir / f"T_PABV23_C_OKPR_{now:%Y%m%d%H%M}00.hdf"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"hdf")
        remote = RemoteInputFile(input_config, now, f"https://example.test/{path.name}", path.name, {})
        local = LocalInputFile(input_config, now, path, remote, downloaded=True)
        calls.append(("sync", now))
        return [InputSyncResult(input=input_config, files=(local,))]

    def fake_sleep(seconds):
        raise KeyboardInterrupt

    runtime = RadarRuntime(config, sleep_func=fake_sleep, now=now)
    runtime.scheduler.sync_func = fake_sync
    runtime.scheduler.render_func = lambda input_index, products: []
    runtime.live_polling_scheduler.sync_func = lambda inputs, *, now=None, limit_per_input=None: []
    runtime.live_polling_scheduler.render_func = lambda input_index, products: []
    runtime.start_mqtt = lambda: calls.append(("start_mqtt", None))
    runtime.stop_mqtt = lambda: calls.append(("stop_mqtt", None))

    try:
        runtime.run_forever()
    except KeyboardInterrupt:
        pass

    assert calls[0] == ("start_mqtt", None)
    assert calls[1][0] == "sync"
    assert calls[-1] == ("stop_mqtt", None)


def test_live_polling_step_polls_non_mqtt_inputs(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    now = datetime(2026, 6, 5, 21, 5)
    calls = []

    def fake_sync(inputs, *, now=None, limit_per_input=None):
        assert limit_per_input == 1
        calls.append(tuple(inputs))
        path = input_config.local_dir / f"T_PABV23_C_OKPR_{now:%Y%m%d%H%M}00.hdf"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"hdf")
        remote = RemoteInputFile(input_config, now, f"https://example.test/{path.name}", path.name, {})
        local = LocalInputFile(input_config, now, path, remote, downloaded=True)
        return [InputSyncResult(input=input_config, files=(local,))]

    runtime = RadarRuntime(config, now=now)
    runtime.mqtt.connected = True
    runtime.live_polling_scheduler.sync_func = fake_sync
    runtime.live_polling_scheduler.render_func = lambda input_index, products: []

    result = runtime.run_live_polling_step(now=now)

    assert result is not None
    assert result.downloaded_count == 1
    assert calls == [(input_config,)]
