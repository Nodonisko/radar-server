from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

from radar_server.config import (
    GeoBounds,
    ProductConfig,
    RenderPipeline,
    RenderProfile,
    cz_maxz,
    timestamped_base,
)
from radar_server.fetching import LocalInputFile, RemoteInputFile
from radar_server.input_index import LocalInputIndex
import radar_server.render_jobs as render_jobs_module
from radar_server.render_jobs import (
    RenderJob,
    RenderInput,
    bounds_tuple,
    expected_output_paths,
    render_job,
    render_ready_jobs,
    resolve_render_jobs,
)
from radar_server.rendering.pipeline import RenderResult


def _with_local_dir(input_config, local_dir: Path):
    return replace(input_config, local_dir=local_dir)


def _product(tmp_path: Path, *, inputs, geo_bounds=None, render=None) -> ProductConfig:
    return ProductConfig(
        id="test",
        label="Test product",
        inputs=tuple(inputs),
        output_dir=tmp_path / "out",
        geo_bounds=geo_bounds or GeoBounds(0, 0, 0, 0),
        base_name=timestamped_base("radar_test"),
        render=render or RenderProfile(),
    )


def _local_file(input_config, timestamp: datetime, path: Path) -> LocalInputFile:
    remote = RemoteInputFile(
        input=input_config,
        timestamp=timestamp,
        url=path.resolve().as_uri(),
        filename=path.name,
        metadata={},
    )
    return LocalInputFile(
        input=input_config,
        timestamp=timestamp,
        path=path,
        remote=remote,
        downloaded=False,
    )


def _index(*files: LocalInputFile) -> LocalInputIndex:
    by_input = {}
    for item in files:
        by_timestamp = by_input.setdefault(item.input.id, {})
        by_timestamp.setdefault(item.timestamp, ())
        by_timestamp[item.timestamp] = (*by_timestamp[item.timestamp], item)
    return LocalInputIndex(files=by_input)


def test_input_index_groups_files_by_input_and_timestamp(tmp_path: Path) -> None:
    ts = datetime(2026, 6, 5, 21, 5)
    input_config = _with_local_dir(cz_maxz, tmp_path / "in")
    path = tmp_path / "a.hdf"
    path.write_bytes(b"x")

    input_index = _index(_local_file(input_config, ts, path))

    assert input_index.timestamps_for(input_config) == {ts}
    assert input_index.files_for(input_config, ts)[0].path == path


def test_startup_scan_uses_input_suffixes_and_two_hour_cutoff(tmp_path: Path) -> None:
    input_config = _with_local_dir(cz_maxz, tmp_path)
    (tmp_path / "T_PABV23_C_OKPR_20260605180500.hdf").write_bytes(b"old")
    recent = tmp_path / "T_PABV23_C_OKPR_20260605190500.hdf"
    recent.write_bytes(b"recent")
    (tmp_path / "T_PABV23_C_OKPR_20260605210500.txt").write_bytes(b"wrong suffix")

    input_index = LocalInputIndex.from_filesystem([input_config], now=datetime(2026, 6, 5, 21, 5))

    assert input_index.timestamps_for(input_config) == {datetime(2026, 6, 5, 19, 5)}
    assert input_index.files_for(input_config, datetime(2026, 6, 5, 19, 5))[0].path == recent


def test_ready_timestamps_requires_all_product_inputs(tmp_path: Path) -> None:
    a = replace(cz_maxz, id="a", local_dir=tmp_path / "a")
    b = replace(cz_maxz, id="b", local_dir=tmp_path / "b")
    ts_ready = datetime(2026, 6, 5, 21, 5)
    ts_missing = datetime(2026, 6, 5, 21, 10)
    product = _product(tmp_path, inputs=(a, b))

    input_index = _index(
        _local_file(a, ts_ready, tmp_path / "a_ready.hdf"),
        _local_file(b, ts_ready, tmp_path / "b_ready.hdf"),
        _local_file(a, ts_missing, tmp_path / "a_missing_b.hdf"),
    )

    assert input_index.ready_timestamps(product) == {ts_ready}


def test_resolve_render_jobs_skips_existing_outputs(tmp_path: Path) -> None:
    ts = datetime(2026, 6, 5, 21, 5)
    input_config = _with_local_dir(cz_maxz, tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    input_path = tmp_path / "input.hdf"
    input_path.write_bytes(b"x")

    input_index = _index(_local_file(input_config, ts, input_path))
    for path in expected_output_paths(product, ts):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"exists")

    assert resolve_render_jobs(input_index, [product]) == []
    assert len(resolve_render_jobs(input_index, [product], include_existing=True)) == 1


def test_render_ready_jobs_renders_every_missing_timestamp(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    timestamps = (
        datetime(2026, 6, 5, 20, 55),
        datetime(2026, 6, 5, 21, 0),
        datetime(2026, 6, 5, 21, 5),
        datetime(2026, 6, 5, 21, 10),
    )
    input_config = _with_local_dir(cz_maxz, tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    input_files = []
    for timestamp in timestamps:
        input_path = tmp_path / f"{timestamp:%H%M}.hdf"
        input_path.write_bytes(b"x")
        input_files.append(_local_file(input_config, timestamp, input_path))
    input_index = _index(*input_files)
    calls = []

    def fake_render_job(job, *, skip_existing=True, on_output_ready=None):  # noqa: ANN001
        calls.append((job.timestamp, skip_existing))
        return None

    monkeypatch.setattr(render_jobs_module, "render_job", fake_render_job)

    results = render_ready_jobs(input_index, [product])

    assert results == []
    assert calls == [(timestamp, True) for timestamp in timestamps]


def test_bounds_tuple_converts_geo_bounds() -> None:
    assert bounds_tuple(GeoBounds(west=1, south=2, east=3, north=4)) == (1, 2, 3, 4)
    assert bounds_tuple(None) is None


def test_render_job_uses_composite_renderer_when_bounds_are_set(tmp_path: Path) -> None:
    calls = []

    def render_single(*args, **kwargs):  # noqa: ANN002, ANN003
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
        calls.append(
            {
                "paths": tuple(paths),
                "output_dir": output_dir,
                "palette": palette,
                "base": base,
                "bounds": bounds,
                "variants": tuple(variants),
                "optimize": optimize,
                "nodata_fill": nodata_fill,
            }
        )
        return RenderResult(base=base, variants={}, sidecar=output_dir / f"{base}.json", bounds=bounds)

    ts = datetime(2026, 6, 5, 21, 5)
    input_config = _with_local_dir(cz_maxz, tmp_path / "in")
    input_path = tmp_path / "input.hdf"
    input_path.write_bytes(b"x")
    product = _product(
        tmp_path,
        inputs=(input_config,),
        geo_bounds=GeoBounds(west=11, south=48, east=19, north=51),
        render=RenderProfile(
            pipeline=RenderPipeline(
                id="fake",
                render_single=render_single,
                render_composite=render_composite,
            )
        ),
    )
    job = RenderJob(
        product=product,
        timestamp=ts,
        inputs=(RenderInput(input=input_config, files=(_local_file(input_config, ts, input_path),)),),
    )

    result = render_job(job, skip_existing=False)

    assert result is not None
    assert calls[0]["paths"] == (input_path,)
    assert calls[0]["bounds"] == (11, 48, 19, 51)
    assert calls[0]["base"] == "radar_test_20260605_2105"


def test_render_job_returns_none_when_outputs_exist(tmp_path: Path) -> None:
    ts = datetime(2026, 6, 5, 21, 5)
    input_config = _with_local_dir(cz_maxz, tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    input_path = tmp_path / "input.hdf"
    input_path.write_bytes(b"x")
    for path in expected_output_paths(product, ts):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"exists")
    job = RenderJob(
        product=product,
        timestamp=ts,
        inputs=(RenderInput(input=input_config, files=(_local_file(input_config, ts, input_path),)),),
    )

    assert render_job(job, skip_existing=True) is None
