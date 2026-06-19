from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from radar_server import forecast_store
from radar_server.config import (
    ForecastProduct,
    GeoBounds,
    ProductConfig,
    RadarServerConfig,
    RenderPipeline,
    RenderProfile,
    chmi_current,
    cz_maxz,
    timestamped_base,
)
from radar_server.fetching import LocalInputFile, RemoteInputFile
from radar_server.mqtt_watcher import MqttNotification
from radar_server.queueing import ForecastRenderTask, MqttIngestTask, PollIngestTask
from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField
from radar_server.rendering.pipeline import RenderResult
from radar_server.runtime import RadarRuntime
from radar_server.workers import ForecastGenPool

NOW = datetime(2026, 6, 5, 21, 5)


class _InlineExecutor:
    def submit(self, fn, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        fn(*args, **kwargs)

    def shutdown(self, wait: bool = False, cancel_futures: bool = False) -> None:
        pass


def _fake_pipeline(calls: list) -> RenderPipeline:
    def render_single(hdf_path, output_dir, palette, *, base, variants=(), optimize=True, nodata_fill=None, on_output_ready=None):  # noqa: ANN001
        raise AssertionError("single renderer should not be used when bounds are set")

    def render_composite(  # noqa: ANN001
        paths,
        output_dir,
        palette,
        *,
        base,
        bounds=None,
        variants=(),
        optimize=True,
        nodata_fill=None,
        on_output_ready=None,
    ):
        calls.append(base)
        return RenderResult(base=base, variants={}, sidecar=output_dir / f"{base}.json", bounds=bounds)

    return RenderPipeline(id="fake", render_single=render_single, render_composite=render_composite)


def _config(tmp_path: Path, calls: list | None = None, *, forecasts=()) -> tuple[RadarServerConfig, object]:
    input_config = replace(cz_maxz, local_dir=tmp_path / "input")
    product = ProductConfig(
        id="test",
        label="Test",
        inputs=(input_config,),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(west=11, south=48, east=19, north=51),
        base_name=timestamped_base("radar_test"),
        render=RenderProfile(pipeline=_fake_pipeline(calls if calls is not None else []), optimize=False),
        priority=0,
    )
    config = RadarServerConfig(
        sources=(chmi_current,),
        inputs=(input_config,),
        products=(product,),
        forecasts=tuple(forecasts),
    )
    return config, input_config


def _write_input_file(input_config, timestamp: datetime) -> Path:  # noqa: ANN001
    path = input_config.local_dir / f"T_PABV23_C_OKPR_{timestamp:%Y%m%d%H%M}00.hdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"hdf")
    return path


def _local_file(input_config, timestamp: datetime, path: Path) -> LocalInputFile:  # noqa: ANN001
    remote = RemoteInputFile(
        input=input_config,
        timestamp=timestamp,
        url=f"https://example.test/{path.name}",
        filename=path.name,
        metadata={},
    )
    return LocalInputFile(input=input_config, timestamp=timestamp, path=path, remote=remote, downloaded=True)


def _radar_field(timestamp: datetime) -> RadarField:
    return RadarField(
        values=np.array([[10.0, 35.0], [40.0, 50.0]], dtype=np.float32),
        crs=WEB_MERCATOR,
        transform=GeoTransform(x_min=0.0, y_max=100.0, px=1.0, py=1.0, width=2, height=2),
        quantity="DBZH",
        timestamp=timestamp,
    )


def test_mqtt_notification_only_enqueues_ingest(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW)
    filename = f"T_PABV23_C_OKPR_{NOW:%Y%m%d%H%M}00.hdf"
    remote = RemoteInputFile(
        input=input_config,
        timestamp=NOW,
        url=f"https://example.test/{filename}",
        filename=filename,
        metadata={},
    )

    runtime._on_mqtt_notification(MqttNotification(topic="t", input=input_config, remotes=(remote,)))

    task = runtime.ingest_queue.get()
    assert isinstance(task, MqttIngestTask)
    assert task.remotes == (remote,)
    # No download or render happened on the (simulated) network thread.
    assert not input_config.local_dir.exists()
    assert runtime.render_queue.is_idle()


def test_mqtt_notification_without_remotes_is_dropped(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW)

    runtime._on_mqtt_notification(MqttNotification(topic="t", input=input_config, remotes=()))

    assert runtime.ingest_queue.is_idle()


def test_enqueue_due_polls_uses_scheduler_decisions(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW - timedelta(minutes=2))

    assert runtime.enqueue_due_polls(now=NOW - timedelta(minutes=2)) == 1  # initial baseline poll
    task = runtime.ingest_queue.get()
    assert isinstance(task, PollIngestTask)
    assert task.input is input_config
    assert task.limit_per_input == 1
    runtime.ingest_queue.task_done(task)

    # Nothing due immediately afterwards.
    assert runtime.enqueue_due_polls(now=NOW - timedelta(minutes=2)) == 0


def test_full_backfill_is_not_deduped_by_pending_live_poll(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW - timedelta(minutes=2))

    assert runtime.enqueue_due_polls(now=NOW - timedelta(minutes=2)) == 1
    assert runtime.enqueue_all_inputs(reason="scheduled_backfill") == 1

    tasks = [runtime.ingest_queue.get(), runtime.ingest_queue.get()]
    assert all(isinstance(task, PollIngestTask) for task in tasks)
    assert [task.input for task in tasks] == [input_config, input_config]
    assert sorted(task.limit_per_input for task in tasks) == [1, 26]


def test_ingest_to_render_flow_end_to_end(tmp_path: Path) -> None:
    calls: list[str] = []
    config, input_config = _config(tmp_path, calls)
    runtime = RadarRuntime(config, now=NOW)

    def fake_sync(cfg, *, now=None, limit=None):  # noqa: ANN001
        path = _write_input_file(cfg, NOW)
        return [_local_file(cfg, NOW, path)]

    runtime.download_worker.sync_func = fake_sync
    runtime.download_worker.now_func = lambda: NOW
    runtime.enqueue_all_inputs(reason="test")

    assert runtime.download_worker.process_one() is True
    assert not runtime.render_queue.is_idle()
    assert runtime.render_workers[0].process_one() is True

    assert calls == ["radar_test_20260605_2105"]
    assert runtime.render_queue.is_idle()


def test_poll_results_feed_scheduler_state(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW)

    def fake_sync(cfg, *, now=None, limit=None):  # noqa: ANN001
        path = _write_input_file(cfg, NOW)
        return [_local_file(cfg, NOW, path)]

    runtime.download_worker.sync_func = fake_sync
    runtime.download_worker.now_func = lambda: NOW

    boundary = datetime(2026, 6, 5, 21, 10)
    assert runtime.enqueue_due_polls(now=boundary) == 1  # enters quick polling
    state = runtime.polling_scheduler.source_states[input_config.source.id]
    assert state.quick_mode is True

    runtime.download_worker.process_one()  # downloads a new file -> leaves quick mode

    assert state.quick_mode is False


def test_reconcile_startup_enqueues_missing_observed_and_forecast_work(tmp_path: Path) -> None:
    calls: list[str] = []
    input_config = replace(cz_maxz, local_dir=tmp_path / "input")
    earlier = NOW - timedelta(minutes=5)
    _write_input_file(input_config, earlier)
    _write_input_file(input_config, NOW)

    product = ProductConfig(
        id="test",
        label="Test",
        inputs=(input_config,),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(west=11, south=48, east=19, north=51),
        base_name=timestamped_base("radar_test"),
        render=RenderProfile(pipeline=_fake_pipeline(calls), optimize=False),
        priority=0,
    )
    forecast = ForecastProduct(
        id="test_forecast",
        parent=product,
        minutes=(10,),
        history_frames=2,
        priority=1000,
        field_dir=tmp_path / "fields",
    )
    # A stored field without rendered outputs survives a restart.
    field_path = forecast_store.field_path(forecast, earlier, 10)
    forecast_store.save_field(_radar_field(earlier), field_path, issue_timestamp=earlier, minute=10)

    config = RadarServerConfig(
        sources=(chmi_current,), inputs=(input_config,), products=(product,), forecasts=(forecast,)
    )
    runtime = RadarRuntime(config, now=NOW)
    runtime.reconcile_startup()

    keys = runtime.render_queue.keys()
    assert ("observed_render", "test", earlier) in keys
    assert ("observed_render", "test", NOW) in keys
    assert ("forecast_render", "test_forecast", earlier, 10) in keys
    wants = runtime.want_board.snapshot()
    assert set(wants) == {"test_forecast"}
    assert wants["test_forecast"].issue_timestamp == NOW


def test_forecast_generation_only_dispatches_when_render_lane_idle(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "input")
    earlier = NOW - timedelta(minutes=5)
    _write_input_file(input_config, earlier)
    _write_input_file(input_config, NOW)

    product = ProductConfig(
        id="test",
        label="Test",
        inputs=(input_config,),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(west=11, south=48, east=19, north=51),
        base_name=timestamped_base("radar_test"),
        render=RenderProfile(pipeline=_fake_pipeline([]), optimize=False),
        priority=0,
    )
    forecast = ForecastProduct(
        id="test_forecast",
        parent=product,
        minutes=(10,),
        history_frames=2,
        priority=1000,
        field_dir=tmp_path / "fields",
    )
    config = RadarServerConfig(
        sources=(chmi_current,), inputs=(input_config,), products=(product,), forecasts=(forecast,)
    )
    runtime = RadarRuntime(config, now=NOW)
    generated: list = []

    def fake_generate(task):  # noqa: ANN001
        generated.append(task)
        return {minute: _radar_field(NOW + timedelta(minutes=minute)) for minute in task.forecast.minutes}

    runtime.forecast_pool = ForecastGenPool(
        runtime.render_queue, generate_func=fake_generate, executor=_InlineExecutor()
    )
    runtime.reconcile_startup()

    # Observed renders pending: render lane is busy, generation must wait.
    assert not runtime.render_queue.is_idle()
    assert runtime.dispatch_forecast_generation() == 0
    assert generated == []

    while runtime.render_workers[0].process_one():
        pass
    assert runtime.render_queue.is_idle()

    # Lane idle: generation runs, writes fields, enqueues the forecast render.
    assert runtime.dispatch_forecast_generation() == 1
    assert len(generated) == 1
    assert generated[0].issue_timestamp == NOW
    task = runtime.render_queue.get()
    assert isinstance(task, ForecastRenderTask)
    assert task.minute == 10
    assert task.field_path.exists()


def test_run_forever_starts_and_shuts_down_cleanly(tmp_path: Path) -> None:
    config, _input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW)
    runtime.download_worker.sync_func = lambda cfg, *, now=None, limit=None: []
    runtime.request_stop()

    runtime.run_forever(sleep_seconds=0.01)

    assert not runtime.download_worker.is_alive()
    assert not any(worker.is_alive() for worker in runtime.render_workers)


def test_run_forever_shuts_down_on_interrupt(tmp_path: Path) -> None:
    config, _input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW)
    runtime.download_worker.sync_func = lambda cfg, *, now=None, limit=None: []

    def interrupting_sleep(seconds: float) -> None:
        raise KeyboardInterrupt

    runtime.sleep_func = interrupting_sleep

    with pytest.raises(KeyboardInterrupt):
        runtime.run_forever(sleep_seconds=0.01)

    assert not runtime.download_worker.is_alive()
    assert not any(worker.is_alive() for worker in runtime.render_workers)


def test_startup_enqueues_full_poll_for_all_inputs(tmp_path: Path) -> None:
    config, input_config = _config(tmp_path)
    runtime = RadarRuntime(config, now=NOW)

    assert runtime.enqueue_all_inputs(reason="startup") == 1
    task = runtime.ingest_queue.get()
    assert isinstance(task, PollIngestTask)
    assert task.reason == "startup"
    assert task.limit_per_input == 26  # 7200s retention / 300s period + 2

    # Duplicate enqueue is a no-op while the first poll is pending.
    runtime.ingest_queue.task_done(task)
    assert runtime.enqueue_all_inputs(reason="startup") == 1
    assert runtime.enqueue_all_inputs(reason="startup") == 0
