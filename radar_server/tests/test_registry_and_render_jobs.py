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
from radar_server.registry import InputRegistry
from radar_server.render_jobs import (
    RenderJob,
    RenderInput,
    bounds_tuple,
    expected_output_paths,
    render_job,
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
        geo_bounds=geo_bounds,
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


def test_registry_groups_files_by_input_and_timestamp(tmp_path: Path) -> None:
    ts = datetime(2026, 6, 5, 21, 5)
    input_config = _with_local_dir(cz_maxz, tmp_path / "in")
    path = tmp_path / "a.hdf"
    path.write_bytes(b"x")

    registry = InputRegistry()
    registry.add([_local_file(input_config, ts, path)])

    assert registry.timestamps_for(input_config) == {ts}
    assert registry.files_for(input_config, ts)[0].path == path


def test_startup_scan_uses_input_suffixes_and_two_hour_cutoff(tmp_path: Path) -> None:
    input_config = _with_local_dir(cz_maxz, tmp_path)
    (tmp_path / "T_PABV23_C_OKPR_20260605180500.hdf").write_bytes(b"old")
    recent = tmp_path / "T_PABV23_C_OKPR_20260605190500.hdf"
    recent.write_bytes(b"recent")
    (tmp_path / "T_PABV23_C_OKPR_20260605210500.txt").write_bytes(b"wrong suffix")

    registry = InputRegistry.from_local_inputs([input_config], now=datetime(2026, 6, 5, 21, 5))

    assert registry.timestamps_for(input_config) == {datetime(2026, 6, 5, 19, 5)}
    assert registry.files_for(input_config, datetime(2026, 6, 5, 19, 5))[0].path == recent


def test_ready_timestamps_requires_all_product_inputs(tmp_path: Path) -> None:
    a = replace(cz_maxz, id="a", local_dir=tmp_path / "a")
    b = replace(cz_maxz, id="b", local_dir=tmp_path / "b")
    ts_ready = datetime(2026, 6, 5, 21, 5)
    ts_missing = datetime(2026, 6, 5, 21, 10)
    product = _product(tmp_path, inputs=(a, b))

    registry = InputRegistry()
    registry.add(
        [
            _local_file(a, ts_ready, tmp_path / "a_ready.hdf"),
            _local_file(b, ts_ready, tmp_path / "b_ready.hdf"),
            _local_file(a, ts_missing, tmp_path / "a_missing_b.hdf"),
        ]
    )

    assert registry.ready_timestamps(product) == {ts_ready}


def test_resolve_render_jobs_skips_existing_outputs(tmp_path: Path) -> None:
    ts = datetime(2026, 6, 5, 21, 5)
    input_config = _with_local_dir(cz_maxz, tmp_path / "in")
    product = _product(tmp_path, inputs=(input_config,))
    input_path = tmp_path / "input.hdf"
    input_path.write_bytes(b"x")

    registry = InputRegistry()
    registry.add([_local_file(input_config, ts, input_path)])
    for path in expected_output_paths(product, ts):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"exists")

    assert resolve_render_jobs(registry, [product]) == []
    assert len(resolve_render_jobs(registry, [product], include_existing=True)) == 1


def test_bounds_tuple_converts_geo_bounds() -> None:
    assert bounds_tuple(GeoBounds(west=1, south=2, east=3, north=4)) == (1, 2, 3, 4)
    assert bounds_tuple(None) is None


def test_render_job_uses_composite_renderer_when_bounds_are_set(tmp_path: Path) -> None:
    calls = []

    def render_single(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("single renderer should not be used when bounds are set")

    def render_composite(paths, output_dir, palette, *, base, bounds=None, variants=(), optimize=True):  # noqa: ANN001
        calls.append(
            {
                "paths": tuple(paths),
                "output_dir": output_dir,
                "palette": palette,
                "base": base,
                "bounds": bounds,
                "variants": tuple(variants),
                "optimize": optimize,
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
