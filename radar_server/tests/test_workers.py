from __future__ import annotations

import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from radar_server import forecast_store
from radar_server.config import (
    ForecastProduct,
    GeoBounds,
    ProductConfig,
    RenderPipeline,
    RenderProfile,
    cz_maxz,
    timestamped_base,
)
from radar_server.fetching import FetchError, LocalInputFile, RemoteInputFile
from radar_server.input_index import LocalInputIndex
from radar_server.queueing import (
    ForecastGenTask,
    ForecastRenderTask,
    HistoryFrame,
    MqttIngestTask,
    ObservedRenderTask,
    PollIngestTask,
    PriorityWorkQueue,
)
from radar_server.render_jobs import RenderInput, RenderJob, expected_output_paths
from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField
from radar_server.rendering.pipeline import RenderResult
from radar_server.workers import (
    DownloadWorker,
    ForecastGenPool,
    ForecastWantBoard,
    IndexHolder,
    RenderWorker,
    build_forecast_gen_task,
    enqueue_observed_render_jobs,
    update_forecast_wants,
)

TS = datetime(2026, 6, 5, 21, 5)


class _InlineExecutor:
    """Runs submitted work synchronously; keeps pool tests deterministic."""

    def submit(self, fn, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        fn(*args, **kwargs)

    def shutdown(self, wait: bool = False, cancel_futures: bool = False) -> None:
        pass


class _ManualExecutor:
    """Captures submitted work so tests can run it while the pool sees it in flight."""

    def __init__(self) -> None:
        self.submissions = []

    def submit(self, fn, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        self.submissions.append((fn, args, kwargs))

    def shutdown(self, wait: bool = False, cancel_futures: bool = False) -> None:
        pass


def _fake_pipeline(calls: list, *, fail_bases: set[str] | None = None) -> RenderPipeline:
    failing = fail_bases or set()

    def render_single(hdf_path, output_dir, palette, *, base, variants=(), optimize=True):  # noqa: ANN001
        raise AssertionError("single renderer should not be used when bounds are set")

    def render_composite(paths, output_dir, palette, *, base, bounds=None, variants=(), optimize=True):  # noqa: ANN001
        if base in failing:
            raise RuntimeError(f"render failed for {base}")
        calls.append(base)
        return RenderResult(base=base, variants={}, sidecar=output_dir / f"{base}.json", bounds=bounds)

    return RenderPipeline(id="fake", render_single=render_single, render_composite=render_composite)


def _product(
    tmp_path: Path,
    *,
    product_id: str = "test",
    priority: int = 0,
    pipeline: RenderPipeline | None = None,
    inputs=None,  # noqa: ANN001
) -> ProductConfig:
    render = RenderProfile(optimize=False) if pipeline is None else RenderProfile(pipeline=pipeline, optimize=False)
    return ProductConfig(
        id=product_id,
        label=product_id,
        inputs=tuple(inputs or (cz_maxz,)),
        output_dir=tmp_path / "out" / product_id,
        geo_bounds=GeoBounds(west=11, south=48, east=19, north=51),
        base_name=timestamped_base(f"radar_{product_id}"),
        render=render,
        priority=priority,
    )


def _local_file(input_config, timestamp: datetime, path: Path, *, downloaded: bool = False) -> LocalInputFile:
    remote = RemoteInputFile(
        input=input_config,
        timestamp=timestamp,
        url=f"https://example.test/{path.name}",
        filename=path.name,
        metadata={},
    )
    return LocalInputFile(input=input_config, timestamp=timestamp, path=path, remote=remote, downloaded=downloaded)


def _job(product: ProductConfig, input_config, timestamp: datetime, path: Path) -> RenderJob:
    return RenderJob(
        product=product,
        timestamp=timestamp,
        inputs=(RenderInput(input=input_config, files=(_local_file(input_config, timestamp, path),)),),
    )


def _radar_field(timestamp: datetime, *, values: np.ndarray | None = None) -> RadarField:
    data = values if values is not None else np.array([[10.0, np.nan], [35.0, 50.0]], dtype=np.float32)
    height, width = data.shape
    return RadarField(
        values=data.astype(np.float32),
        crs=WEB_MERCATOR,
        transform=GeoTransform(x_min=0.0, y_max=100.0, px=1.0, py=1.0, width=width, height=height),
        quantity="DBZH",
        timestamp=timestamp,
    )


def _forecast(tmp_path: Path, parent: ProductConfig, *, minutes=(10, 20), history_frames: int = 2) -> ForecastProduct:
    return ForecastProduct(
        id=f"{parent.id}_forecast",
        parent=parent,
        minutes=tuple(minutes),
        history_frames=history_frames,
        priority=1000,
        field_dir=tmp_path / "fields" / parent.id,
    )


# RenderWorker ----------------------------------------------------------------


def test_render_worker_executes_in_priority_order(tmp_path: Path) -> None:
    calls: list[str] = []
    pipeline = _fake_pipeline(calls)
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    high = _product(tmp_path, product_id="cz", priority=0, pipeline=pipeline, inputs=(input_config,))
    low = _product(tmp_path, product_id="de", priority=10, pipeline=pipeline, inputs=(input_config,))
    input_path = tmp_path / "input.hdf"
    input_path.write_bytes(b"x")

    render_queue = PriorityWorkQueue()
    worker = RenderWorker(render_queue)
    # Enqueue the low-priority product first; the high-priority one must win.
    render_queue.put_if_absent(low.priority, ObservedRenderTask(job=_job(low, input_config, TS, input_path)))
    render_queue.put_if_absent(high.priority, ObservedRenderTask(job=_job(high, input_config, TS, input_path)))

    while worker.process_one():
        pass

    assert calls == ["radar_cz_20260605_2105", "radar_de_20260605_2105"]
    assert render_queue.is_idle()


def test_render_worker_skips_when_outputs_exist(tmp_path: Path) -> None:
    calls: list[str] = []
    pipeline = _fake_pipeline(calls)
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, pipeline=pipeline, inputs=(input_config,))
    input_path = tmp_path / "input.hdf"
    input_path.write_bytes(b"x")
    for path in expected_output_paths(product, TS):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"exists")

    render_queue = PriorityWorkQueue()
    render_queue.put_if_absent(0, ObservedRenderTask(job=_job(product, input_config, TS, input_path)))
    RenderWorker(render_queue).process_one()

    assert calls == []


def test_render_worker_skips_missing_input_files_without_crashing(tmp_path: Path) -> None:
    calls: list[str] = []
    pipeline = _fake_pipeline(calls)
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, pipeline=pipeline, inputs=(input_config,))

    render_queue = PriorityWorkQueue()
    render_queue.put_if_absent(0, ObservedRenderTask(job=_job(product, input_config, TS, tmp_path / "pruned.hdf")))
    worker = RenderWorker(render_queue)

    assert worker.process_one() is True
    assert calls == []
    assert render_queue.is_idle()


def test_render_worker_survives_a_failing_task(tmp_path: Path) -> None:
    calls: list[str] = []
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    pipeline = _fake_pipeline(calls, fail_bases={"radar_bad_20260605_2105"})
    bad = _product(tmp_path, product_id="bad", priority=0, pipeline=pipeline, inputs=(input_config,))
    good = _product(tmp_path, product_id="good", priority=10, pipeline=pipeline, inputs=(input_config,))
    input_path = tmp_path / "input.hdf"
    input_path.write_bytes(b"x")

    render_queue = PriorityWorkQueue()
    render_queue.put_if_absent(bad.priority, ObservedRenderTask(job=_job(bad, input_config, TS, input_path)))
    render_queue.put_if_absent(good.priority, ObservedRenderTask(job=_job(good, input_config, TS, input_path)))
    worker = RenderWorker(render_queue)

    while worker.process_one():
        pass

    assert calls == ["radar_good_20260605_2105"]
    assert render_queue.is_idle()


def test_render_worker_renders_forecast_frame_from_stored_field(tmp_path: Path) -> None:
    parent = _product(tmp_path)
    forecast = _forecast(tmp_path, parent)
    field_path = forecast_store.field_path(forecast, TS, 10)
    forecast_store.save_field(_radar_field(TS + timedelta(minutes=10)), field_path, issue_timestamp=TS, minute=10)
    base = forecast_store.forecast_base(forecast, TS, 10)

    render_queue = PriorityWorkQueue()
    render_queue.put_if_absent(
        forecast.priority,
        ForecastRenderTask(forecast=forecast, issue_timestamp=TS, minute=10, field_path=field_path, base=base),
    )
    RenderWorker(render_queue).process_one()

    for path in forecast_store.frame_output_paths(forecast, base):
        assert path.exists(), path


def test_render_worker_skips_forecast_frame_when_field_missing(tmp_path: Path) -> None:
    parent = _product(tmp_path)
    forecast = _forecast(tmp_path, parent)
    base = forecast_store.forecast_base(forecast, TS, 10)

    render_queue = PriorityWorkQueue()
    render_queue.put_if_absent(
        forecast.priority,
        ForecastRenderTask(
            forecast=forecast,
            issue_timestamp=TS,
            minute=10,
            field_path=forecast_store.field_path(forecast, TS, 10),  # never written (superseded)
            base=base,
        ),
    )
    worker = RenderWorker(render_queue)

    assert worker.process_one() is True
    assert not any(path.exists() for path in forecast_store.frame_output_paths(forecast, base))


def test_render_worker_skips_corrupt_forecast_field(tmp_path: Path) -> None:
    parent = _product(tmp_path)
    forecast = _forecast(tmp_path, parent)
    field_path = forecast_store.field_path(forecast, TS, 10)
    field_path.parent.mkdir(parents=True, exist_ok=True)
    field_path.write_bytes(b"not an npz")
    base = forecast_store.forecast_base(forecast, TS, 10)

    render_queue = PriorityWorkQueue()
    render_queue.put_if_absent(
        forecast.priority,
        ForecastRenderTask(forecast=forecast, issue_timestamp=TS, minute=10, field_path=field_path, base=base),
    )
    worker = RenderWorker(render_queue)

    assert worker.process_one() is True
    assert not any(path.exists() for path in forecast_store.frame_output_paths(forecast, base))


def test_render_worker_skips_superseded_forecast_frame(tmp_path: Path) -> None:
    parent = _product(tmp_path)
    forecast = _forecast(tmp_path, parent)
    old_issue = TS - timedelta(minutes=5)
    field_path = forecast_store.field_path(forecast, old_issue, 10)
    forecast_store.save_field(
        _radar_field(old_issue + timedelta(minutes=10)),
        field_path,
        issue_timestamp=old_issue,
        minute=10,
    )
    base = forecast_store.forecast_base(forecast, old_issue, 10)
    board = ForecastWantBoard()
    board.set(_gen_task(forecast, TS))

    render_queue = PriorityWorkQueue()
    render_queue.put_if_absent(
        forecast.priority,
        ForecastRenderTask(
            forecast=forecast,
            issue_timestamp=old_issue,
            minute=10,
            field_path=field_path,
            base=base,
        ),
    )
    worker = RenderWorker(render_queue, want_board=board)

    assert worker.process_one() is True
    assert not any(path.exists() for path in forecast_store.frame_output_paths(forecast, base))


# DownloadWorker ----------------------------------------------------------------


def _write_input_file(input_config, timestamp: datetime) -> Path:
    path = input_config.local_dir / f"T_PABV23_C_OKPR_{timestamp:%Y%m%d%H%M}00.hdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"hdf")
    return path


def _download_worker(tmp_path: Path, input_config, products, forecasts=()):  # noqa: ANN001
    ingest_queue = PriorityWorkQueue()
    render_queue = PriorityWorkQueue()
    holder = IndexHolder((input_config,), now=TS)
    board = ForecastWantBoard()
    records: list[tuple[str, bool]] = []
    worker = DownloadWorker(
        ingest_queue,
        render_queue,
        products=products,
        forecasts=forecasts,
        index_holder=holder,
        want_board=board,
        record_poll_result=lambda task, has_new, now: records.append((task.input.id, has_new)),
    )
    worker.now_func = lambda: TS
    return worker, ingest_queue, render_queue, holder, board, records


def test_download_worker_poll_enqueues_render_and_records_result(tmp_path: Path) -> None:
    calls: list[str] = []
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, pipeline=_fake_pipeline(calls), inputs=(input_config,))
    worker, ingest_queue, render_queue, holder, _board, records = _download_worker(
        tmp_path, input_config, (product,)
    )

    def fake_sync(config, *, now=None, limit=None):  # noqa: ANN001
        path = _write_input_file(config, TS)
        return [_local_file(config, TS, path, downloaded=True)]

    worker.sync_func = fake_sync
    ingest_queue.put_if_absent(0, PollIngestTask(input=input_config, reason="live_poll", limit_per_input=1))

    assert worker.process_one() is True
    assert records == [(input_config.id, True)]
    assert holder.get().timestamps_for(input_config) == {TS}
    task = render_queue.get()
    assert isinstance(task, ObservedRenderTask)
    assert task.timestamp == TS


def test_download_worker_isolates_fetch_errors(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, pipeline=_fake_pipeline([]), inputs=(input_config,))
    worker, ingest_queue, render_queue, _holder, _board, records = _download_worker(
        tmp_path, input_config, (product,)
    )

    def failing_sync(config, *, now=None, limit=None):  # noqa: ANN001
        raise FetchError("boom")

    worker.sync_func = failing_sync
    ingest_queue.put_if_absent(0, PollIngestTask(input=input_config, reason="live_poll"))

    assert worker.process_one() is True
    assert records == [(input_config.id, False)]
    assert render_queue.is_idle()
    assert ingest_queue.is_idle()


def test_download_worker_handles_mqtt_ingest(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, pipeline=_fake_pipeline([]), inputs=(input_config,))
    worker, ingest_queue, render_queue, _holder, _board, _records = _download_worker(
        tmp_path, input_config, (product,)
    )
    filename = f"T_PABV23_C_OKPR_{TS:%Y%m%d%H%M}00.hdf"
    remote = RemoteInputFile(
        input=input_config,
        timestamp=TS,
        url=f"https://example.test/{filename}",
        filename=filename,
        metadata={},
    )

    def fake_download(remote_file):  # noqa: ANN001
        path = _write_input_file(remote_file.input, remote_file.timestamp)
        return _local_file(remote_file.input, remote_file.timestamp, path, downloaded=True)

    worker.download_func = fake_download
    ingest_queue.put_if_absent(0, MqttIngestTask(input=input_config, remotes=(remote,)))

    assert worker.process_one() is True
    task = render_queue.get()
    assert isinstance(task, ObservedRenderTask)
    assert task.timestamp == TS


def test_download_worker_updates_forecast_wants_when_history_ready(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, pipeline=_fake_pipeline([]), inputs=(input_config,))
    forecast = _forecast(tmp_path, product, history_frames=2)
    worker, ingest_queue, _render_queue, _holder, board, _records = _download_worker(
        tmp_path, input_config, (product,), forecasts=(forecast,)
    )
    earlier = TS - timedelta(minutes=5)
    _write_input_file(input_config, earlier)

    def fake_sync(config, *, now=None, limit=None):  # noqa: ANN001
        path = _write_input_file(config, TS)
        return [_local_file(config, TS, path, downloaded=True)]

    worker.sync_func = fake_sync
    ingest_queue.put_if_absent(0, PollIngestTask(input=input_config, reason="live_poll"))
    worker.process_one()

    wanted = board.snapshot()
    assert set(wanted) == {forecast.id}
    task = wanted[forecast.id]
    assert task.issue_timestamp == TS
    assert [frame.timestamp for frame in task.history] == [earlier, TS]
    assert all(frame.paths for frame in task.history)


# build_forecast_gen_task ------------------------------------------------------


def _index_with_files(input_config, timestamps) -> LocalInputIndex:  # noqa: ANN001
    for timestamp in timestamps:
        _write_input_file(input_config, timestamp)
    return LocalInputIndex.from_filesystem((input_config,), now=max(timestamps))


def test_build_forecast_gen_task_requires_history(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    forecast = _forecast(tmp_path, product, history_frames=3)
    index = _index_with_files(input_config, [TS - timedelta(minutes=5), TS])

    assert build_forecast_gen_task(forecast, index) is None


def test_build_forecast_gen_task_skips_when_outputs_exist(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    forecast = _forecast(tmp_path, product, history_frames=2)
    index = _index_with_files(input_config, [TS - timedelta(minutes=5), TS])
    for path in forecast_store.expected_forecast_paths(forecast, TS):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"exists")

    assert build_forecast_gen_task(forecast, index) is None


def test_build_forecast_gen_task_skips_disabled(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    forecast = replace(_forecast(tmp_path, product), enabled=False)
    index = _index_with_files(input_config, [TS - timedelta(minutes=5), TS])

    assert build_forecast_gen_task(forecast, index) is None


# ForecastWantBoard ------------------------------------------------------------


def _gen_task(forecast: ForecastProduct, issue: datetime) -> ForecastGenTask:
    return ForecastGenTask(
        forecast=forecast,
        issue_timestamp=issue,
        history=(HistoryFrame(timestamp=issue, paths=(Path("dummy.hdf"),)),),
    )


def test_want_board_coalesces_to_latest_issue(tmp_path: Path) -> None:
    product = _product(tmp_path)
    forecast = _forecast(tmp_path, product)
    board = ForecastWantBoard()
    board.set(_gen_task(forecast, TS - timedelta(minutes=5)))
    board.set(_gen_task(forecast, TS))

    task = board.pop_next()

    assert task is not None
    assert task.issue_timestamp == TS  # older issue dropped
    assert board.pop_next() is None


def test_want_board_pops_by_priority_and_respects_exclusions(tmp_path: Path) -> None:
    product_a = _product(tmp_path, product_id="a")
    product_b = _product(tmp_path, product_id="b")
    forecast_a = replace(_forecast(tmp_path, product_a), priority=1000)
    forecast_b = replace(_forecast(tmp_path, product_b), priority=1010)
    board = ForecastWantBoard()
    board.set(_gen_task(forecast_b, TS))
    board.set(_gen_task(forecast_a, TS))

    first = board.pop_next(exclude=())
    assert first is not None and first.forecast.id == forecast_a.id

    board.set(_gen_task(forecast_a, TS))
    excluded = board.pop_next(exclude={forecast_a.id})
    assert excluded is not None and excluded.forecast.id == forecast_b.id


# ForecastGenPool ----------------------------------------------------------------


def test_gen_pool_writes_fields_and_enqueues_lowest_priority_renders(tmp_path: Path) -> None:
    product = _product(tmp_path)
    forecast = _forecast(tmp_path, product, minutes=(10, 20))
    render_queue = PriorityWorkQueue()

    def fake_generate(task):  # noqa: ANN001
        return {minute: _radar_field(TS + timedelta(minutes=minute)) for minute in task.forecast.minutes}

    pool = ForecastGenPool(render_queue, generate_func=fake_generate, executor=_InlineExecutor())
    board = ForecastWantBoard()
    board.set(_gen_task(forecast, TS))

    assert pool.dispatch(board) == 1
    assert pool.in_flight_count() == 0

    for minute in forecast.minutes:
        assert forecast_store.field_path(forecast, TS, minute).exists()

    tasks = [render_queue.get() for _ in range(2)]
    assert all(isinstance(task, ForecastRenderTask) for task in tasks)
    assert [task.minute for task in tasks] == [10, 20]


def test_gen_pool_skips_generation_when_outputs_exist(tmp_path: Path) -> None:
    product = _product(tmp_path)
    forecast = _forecast(tmp_path, product)
    render_queue = PriorityWorkQueue()
    generated: list = []
    for path in forecast_store.expected_forecast_paths(forecast, TS):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"exists")

    pool = ForecastGenPool(render_queue, generate_func=lambda task: generated.append(task), executor=_InlineExecutor())
    board = ForecastWantBoard()
    board.set(_gen_task(forecast, TS))
    pool.dispatch(board)

    assert generated == []
    assert render_queue.is_idle()


def test_gen_pool_reuses_existing_fields_after_crash(tmp_path: Path) -> None:
    product = _product(tmp_path)
    forecast = _forecast(tmp_path, product, minutes=(10, 20))
    render_queue = PriorityWorkQueue()
    forecast_store.write_forecast_fields(
        forecast,
        TS,
        {minute: _radar_field(TS + timedelta(minutes=minute)) for minute in forecast.minutes},
    )
    generated: list = []

    pool = ForecastGenPool(render_queue, generate_func=lambda task: generated.append(task), executor=_InlineExecutor())
    board = ForecastWantBoard()
    board.set(_gen_task(forecast, TS))
    pool.dispatch(board)

    assert generated == []  # fields reused, no recompute
    assert render_queue.pending_count() == 2


def test_gen_pool_discards_generation_superseded_while_running(tmp_path: Path) -> None:
    product = _product(tmp_path)
    forecast = _forecast(tmp_path, product, minutes=(10,))
    old_issue = TS - timedelta(minutes=5)
    render_queue = PriorityWorkQueue()
    board = ForecastWantBoard()
    board.set(_gen_task(forecast, old_issue))

    def fake_generate(task):  # noqa: ANN001
        board.set(_gen_task(forecast, TS))
        return {10: _radar_field(old_issue + timedelta(minutes=10))}

    executor = _ManualExecutor()
    pool = ForecastGenPool(render_queue, generate_func=fake_generate, executor=executor)

    assert pool.dispatch(board) == 1
    assert pool.in_flight_count() == 1

    fn, args, kwargs = executor.submissions.pop()
    fn(*args, **kwargs)

    assert not forecast_store.field_path(forecast, old_issue, 10).exists()
    assert render_queue.is_idle()
    assert pool.in_flight_count() == 0
    assert board.snapshot()[forecast.id].issue_timestamp == TS


def test_gen_pool_isolates_generation_failures(tmp_path: Path) -> None:
    product = _product(tmp_path)
    forecast = _forecast(tmp_path, product)
    render_queue = PriorityWorkQueue()

    def failing_generate(task):  # noqa: ANN001
        raise RuntimeError("pysteps exploded")

    pool = ForecastGenPool(render_queue, generate_func=failing_generate, executor=_InlineExecutor())
    board = ForecastWantBoard()
    board.set(_gen_task(forecast, TS))

    assert pool.dispatch(board) == 1
    assert pool.in_flight_count() == 0
    assert render_queue.is_idle()


def test_gen_pool_runs_at_most_max_workers_concurrently(tmp_path: Path) -> None:
    products = [_product(tmp_path, product_id=name) for name in ("a", "b", "c")]
    forecasts = [_forecast(tmp_path, product) for product in products]
    render_queue = PriorityWorkQueue()
    release = threading.Event()
    started: list[str] = []

    def blocking_generate(task):  # noqa: ANN001
        started.append(task.forecast.id)
        assert release.wait(timeout=5)
        return {}

    pool = ForecastGenPool(render_queue, max_workers=2, generate_func=blocking_generate)
    board = ForecastWantBoard()
    for forecast in forecasts:
        board.set(_gen_task(forecast, TS))

    try:
        assert pool.dispatch(board) == 2
        deadline = time.monotonic() + 5
        while len(started) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(started) == 2
        assert pool.in_flight_count() == 2
        assert len(board) == 1  # third stays parked
        assert pool.dispatch(board) == 0  # no free slot
    finally:
        release.set()

    deadline = time.monotonic() + 5
    while pool.in_flight_count() > 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert pool.in_flight_count() == 0
    assert pool.dispatch(board) == 1  # third forecast now runs
    deadline = time.monotonic() + 5
    while pool.in_flight_count() > 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    pool.shutdown()


# enqueue helpers ----------------------------------------------------------------


def test_enqueue_observed_render_jobs_orders_newest_first_within_priority(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    older = TS - timedelta(minutes=5)
    index = _index_with_files(input_config, [older, TS])
    render_queue = PriorityWorkQueue()

    assert enqueue_observed_render_jobs(render_queue, index, (product,)) == 2

    first = render_queue.get()
    second = render_queue.get()
    assert first.timestamp == TS
    assert second.timestamp == older


def test_update_forecast_wants_sets_only_ready_forecasts(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    ready = _forecast(tmp_path, product, history_frames=2)
    not_ready = replace(_forecast(tmp_path, product), id="needs_more", history_frames=5)
    index = _index_with_files(input_config, [TS - timedelta(minutes=5), TS])
    board = ForecastWantBoard()

    assert update_forecast_wants(board, index, (ready, not_ready)) == 1
    assert set(board.snapshot()) == {ready.id}
