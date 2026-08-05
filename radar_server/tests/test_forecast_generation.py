from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

import radar_server.forecast_generation as forecast_generation
from radar_server.config import (
    ForecastProduct,
    GeoBounds,
    GeoCenter,
    LAYERED_RENDER,
    ProductConfig,
    cz_maxz,
    timestamped_base,
)
from radar_server.forecast_generation import (
    generate_for_task,
    generate_forecast_fields,
    generate_layered_forecast_fields,
    load_layered_history,
)
from radar_server.queueing import ForecastGenTask, HistoryFrame
from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField
from radar_server.rendering.layered import LayeredFrame


def _field(values: np.ndarray, timestamp: datetime) -> RadarField:
    height, width = values.shape
    return RadarField(
        values=values.astype(np.float32),
        crs=WEB_MERCATOR,
        transform=GeoTransform(x_min=0.0, y_max=100.0, px=1.0, py=1.0, width=width, height=height),
        quantity="DBZH",
        timestamp=timestamp,
    )


def _patch_pysteps(monkeypatch, captured: dict, lead_values: tuple[float, ...]) -> None:  # noqa: ANN001
    def fake_motion_method(method: str):
        captured["method"] = method

        def fake_motion(input_images, **kwargs):  # noqa: ANN001, ANN003
            captured["motion_input"] = input_images
            captured["motion_kwargs"] = kwargs
            return np.zeros((2, *input_images.shape[1:]), dtype=np.float32)

        return fake_motion

    def fake_extrapolation_method():
        def fake_extrapolate(precip, velocity, timesteps):  # noqa: ANN001
            captured["precip"] = np.array(precip, copy=True)
            captured["timesteps"] = tuple(timesteps)
            return np.stack([np.full_like(precip, value, dtype=np.float32) for value in lead_values])

        return fake_extrapolate

    monkeypatch.setattr(forecast_generation, "_motion_method", fake_motion_method)
    monkeypatch.setattr(forecast_generation, "_extrapolation_method", fake_extrapolation_method)


def test_generate_forecast_fields_masks_nan_and_uses_lead_steps(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}
    _patch_pysteps(monkeypatch, captured, lead_values=(20.0, 30.0))
    timestamp = datetime(2026, 6, 5, 21, 0)
    fields = [
        _field(np.array([[10.0, np.nan], [15.0, 20.0]]), timestamp),
        _field(np.array([[12.0, np.nan], [18.0, 22.0]]), timestamp + timedelta(minutes=7)),
    ]

    generated = generate_forecast_fields(fields, minutes=(20, 10), method="lucaskanade")

    assert captured["method"] == "lucaskanade"
    assert np.ma.isMaskedArray(captured["motion_input"])
    assert captured["motion_input"].mask[0, 0, 1]
    assert np.isnan(captured["precip"][0, 1])
    assert captured["timesteps"] == (10 / 7, 20 / 7)
    assert sorted(generated) == [10, 20]
    assert generated[10].timestamp == timestamp + timedelta(minutes=17)
    assert generated[20].timestamp == timestamp + timedelta(minutes=27)
    assert generated[10].transform == fields[-1].transform
    assert generated[10].crs == WEB_MERCATOR


def test_generate_forecast_fields_logs_forecast_id(monkeypatch, caplog: pytest.LogCaptureFixture) -> None:  # noqa: ANN001
    captured: dict = {}
    _patch_pysteps(monkeypatch, captured, lead_values=(10.0, 20.0))
    caplog.set_level(logging.INFO)

    t0 = datetime(2026, 6, 5, 21, 0)
    t1 = t0 + timedelta(minutes=5)
    fields = [_field(np.full((4, 4), 30.0), t0), _field(np.full((4, 4), 35.0), t1)]
    generate_forecast_fields(fields, minutes=(10, 20), forecast_id="cz_forecast")

    generation_logs = [
        record.getMessage()
        for record in caplog.records
        if record.name == "radar_server.forecast_generation" and record.levelno == logging.INFO
    ]
    assert len(generation_logs) == 1
    assert generation_logs[0].startswith("Generated cz_forecast forecast fields in ")
    assert "motion=" in generation_logs[0]
    assert "extrapolate=" in generation_logs[0]


def test_generate_forecast_fields_applies_floor_level(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}
    _patch_pysteps(monkeypatch, captured, lead_values=(20.0, 30.0))
    timestamp = datetime(2026, 6, 5, 21, 0)
    fields = [
        _field(np.full((2, 2), 10.0), timestamp),
        _field(np.full((2, 2), 12.0), timestamp + timedelta(minutes=5)),
    ]

    generated = generate_forecast_fields(fields, minutes=(10, 20), floor_level=20.0)

    assert np.isnan(generated[10].values).all()  # 20.0 <= floor -> transparent
    assert (generated[20].values == 30.0).all()


def test_generate_forecast_fields_handles_empty_latest_field(monkeypatch) -> None:  # noqa: ANN001
    def fail_motion_method(method: str):  # noqa: ANN001
        raise AssertionError("motion should not run for an empty latest field")

    def fail_extrapolation_method():
        raise AssertionError("extrapolation should not run for an empty latest field")

    monkeypatch.setattr(forecast_generation, "_motion_method", fail_motion_method)
    monkeypatch.setattr(forecast_generation, "_extrapolation_method", fail_extrapolation_method)
    timestamp = datetime(2026, 6, 5, 21, 0)
    fields = [
        _field(np.array([[10.0, np.nan], [15.0, 20.0]]), timestamp),
        _field(np.full((2, 2), np.nan), timestamp + timedelta(minutes=5)),
    ]

    generated = generate_forecast_fields(fields, minutes=(10, 20))

    assert sorted(generated) == [10, 20]
    assert all(np.isnan(field.values).all() for field in generated.values())
    assert generated[10].timestamp == timestamp + timedelta(minutes=15)
    assert generated[20].timestamp == timestamp + timedelta(minutes=25)
    assert generated[10].transform == fields[-1].transform


def test_generate_forecast_fields_dedupes_minutes(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}
    _patch_pysteps(monkeypatch, captured, lead_values=(20.0,))
    timestamp = datetime(2026, 6, 5, 21, 0)
    fields = [
        _field(np.zeros((2, 2)), timestamp),
        _field(np.zeros((2, 2)), timestamp + timedelta(minutes=5)),
    ]

    generated = generate_forecast_fields(fields, minutes=(10, 10))

    assert sorted(generated) == [10]
    assert captured["timesteps"] == (2.0,)


def test_generate_forecast_fields_validates_inputs(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}
    _patch_pysteps(monkeypatch, captured, lead_values=(20.0,))
    timestamp = datetime(2026, 6, 5, 21, 0)
    one_field = [_field(np.zeros((2, 2)), timestamp)]
    two_fields = [
        _field(np.zeros((2, 2)), timestamp),
        _field(np.zeros((2, 2)), timestamp + timedelta(minutes=5)),
    ]
    reversed_fields = list(reversed(two_fields))

    with pytest.raises(ValueError, match="at least 2"):
        generate_forecast_fields(one_field, minutes=(10,))
    with pytest.raises(ValueError, match="positive"):
        generate_forecast_fields(two_fields, minutes=(0,))
    with pytest.raises(ValueError, match="chronologically"):
        generate_forecast_fields(reversed_fields, minutes=(10,))
    assert generate_forecast_fields(two_fields, minutes=()) == {}


def test_generate_for_task_wires_forecast_settings(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    parent = ProductConfig(
        id="test",
        label="Test",
        inputs=(cz_maxz,),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(0, 0, 0, 0),
        center=GeoCenter(0.0, 0.0),
        publish_delay_seconds=260,
        base_name=timestamped_base("radar_test"),
    )
    forecast = ForecastProduct(
        id="test_forecast",
        parent=parent,
        minutes=(10,),
        method="proesmans",
        field_dir=tmp_path / "fields",
        motion_grid_step=3,
    )
    issue = datetime(2026, 6, 5, 21, 0)
    task = ForecastGenTask(
        forecast=forecast,
        issue_timestamp=issue,
        history=(HistoryFrame(timestamp=issue, paths=(tmp_path / "a.hdf",)),),
    )
    history_fields = [
        _field(np.zeros((2, 2)), issue - timedelta(minutes=5)),
        _field(np.zeros((2, 2)), issue),
    ]
    captured: dict = {}

    def fake_load_history(load_task):  # noqa: ANN001
        captured["task"] = load_task
        return history_fields

    def fake_generate(fields, *, minutes, method, floor_level, **kwargs):  # noqa: ANN001, ANN003
        captured["fields"] = fields
        captured["minutes"] = minutes
        captured["generation_method"] = method
        captured["floor_level"] = floor_level
        captured.update(kwargs)
        return {10: fields[-1]}

    monkeypatch.setattr(forecast_generation, "load_history_fields", fake_load_history)
    monkeypatch.setattr(forecast_generation, "generate_forecast_fields", fake_generate)

    result = generate_for_task(task)

    assert result == {10: history_fields[-1]}
    assert captured["task"] is task
    assert captured["fields"] is history_fields
    assert captured["minutes"] == (10,)
    assert captured["generation_method"] == "proesmans"
    assert captured["floor_level"] == forecast.palette.levels[0]
    assert captured["motion_grid_step"] == 3
    assert captured["motion_grid_max"] == forecast.motion_grid_max
    assert captured["fast_idw"] == forecast.fast_idw
    assert captured["fast_warp"] == forecast.fast_warp
    assert captured["fast_motion"] == forecast.fast_motion
    assert captured["warp_grid_step"] == forecast.warp_grid_step


def test_generate_for_task_routes_layered_parent(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    parent = ProductConfig(
        id="cz",
        label="Czechia",
        inputs=(cz_maxz, cz_maxz),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(0, 0, 0, 0),
        center=GeoCenter(0.0, 0.0),
        publish_delay_seconds=10,
        base_name=timestamped_base("radar_cz"),
        render=LAYERED_RENDER,
    )
    forecast = ForecastProduct(id="cz_forecast", parent=parent, minutes=(10,), field_dir=tmp_path / "fields")
    issue = datetime(2026, 6, 5, 21, 0)
    task = ForecastGenTask(
        forecast=forecast,
        issue_timestamp=issue,
        history=(HistoryFrame(timestamp=issue, paths=(tmp_path / "a.hdf", tmp_path / "b.hdf")),),
    )
    captured: dict = {}
    overlay_history = [
        _field(np.zeros((2, 2)), issue - timedelta(minutes=5)),
        _field(np.zeros((2, 2)), issue),
    ]
    base_latest = _field(np.zeros((2, 2)), issue)

    def fake_load_layered(load_task):  # noqa: ANN001
        captured["layered_task"] = load_task
        return overlay_history, base_latest

    def fake_generate_layered(history, base, *, minutes, **kwargs):  # noqa: ANN001, ANN003
        captured["history"] = history
        captured["base"] = base
        captured["minutes"] = minutes
        captured.update(kwargs)
        return {10: _field(np.zeros((2, 2)), issue)}

    monkeypatch.setattr(forecast_generation, "load_layered_history", fake_load_layered)
    monkeypatch.setattr(forecast_generation, "generate_layered_forecast_fields", fake_generate_layered)

    result = generate_for_task(task)

    assert captured["layered_task"] is task
    assert captured["history"] is overlay_history
    assert captured["base"] is base_latest
    assert captured["minutes"] == (10,)
    assert captured["forecast_id"] == "cz_forecast"
    assert sorted(result) == [10]


_MERC = (
    "+proj=merc +lat_ts=0 +lon_0=0 +k=1.0 +x_0=-1254222.15 +y_0=-6702777.85 "
    "+a=6378137.0 +b=6378137.0 +units=m +nadgrids=@null +wktext +no_defs"
)


def _write_odim(path: Path, raw: np.ndarray, *, product: bytes, time: bytes) -> Path:
    h5py = pytest.importorskip("h5py")
    height, width = raw.shape
    with h5py.File(path, "w") as hdf:
        hdf.attrs["Conventions"] = b"ODIM_H5/V2_4"
        what = hdf.create_group("what")
        what.attrs["date"] = b"20260605"
        what.attrs["time"] = time
        what.attrs["object"] = b"COMP"
        where = hdf.create_group("where")
        where.attrs["projdef"] = _MERC.encode()
        where.attrs["xsize"] = width
        where.attrs["ysize"] = height
        where.attrs["xscale"] = 1555.7
        where.attrs["yscale"] = 1555.7
        where.attrs["UL_lon"] = 11.2669
        where.attrs["UL_lat"] = 51.4584
        dataset = hdf.create_group("dataset1")
        dataset.create_group("what").attrs["product"] = product
        data1 = dataset.create_group("data1")
        data1.create_dataset("data", data=raw)
        dwhat = data1.create_group("what")
        dwhat.attrs["gain"] = 0.5
        dwhat.attrs["offset"] = -32.0
        dwhat.attrs["nodata"] = 255.0
        dwhat.attrs["undetect"] = 0.0
        dwhat.attrs["quantity"] = b"DBZH"
    return path


def _layered_forecast(tmp_path: Path) -> ForecastProduct:
    parent = ProductConfig(
        id="cz",
        label="Czechia",
        inputs=(cz_maxz, cz_maxz),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(west=11.28, south=51.32, east=11.48, north=51.45),
        center=GeoCenter(11.38, 51.39),
        publish_delay_seconds=10,
        base_name=timestamped_base("radar_cz"),
        render=LAYERED_RENDER,
    )
    return ForecastProduct(id="cz_forecast", parent=parent, minutes=(10,), field_dir=tmp_path / "fields")


def test_load_layered_history_streams_overlays_and_latest_base(tmp_path: Path) -> None:
    forecast = _layered_forecast(tmp_path)
    t0 = datetime(2026, 6, 5, 20, 55)
    t1 = datetime(2026, 6, 5, 21, 0)
    shape = (12, 16)

    def _raw(value: int) -> np.ndarray:
        raw = np.zeros(shape, dtype=np.uint8)
        raw[3:9, 2:12] = value
        return raw

    # Older frame: the base file is not a valid HDF at all, proving the loader
    # only ever decodes overlay files for non-latest frames.
    stale_base = tmp_path / "old_base.hdf"
    stale_base.write_bytes(b"never decoded")
    overlay_t0 = _write_odim(tmp_path / "maxz_t0.hdf", _raw(80), product=b"MAX", time=b"205500")  # 8 dBZ
    base_t1 = _write_odim(tmp_path / "cappi_t1.hdf", _raw(160), product=b"PCAPPI", time=b"210000")  # 48 dBZ
    overlay_t1 = _write_odim(tmp_path / "maxz_t1.hdf", _raw(120), product=b"MAX", time=b"210000")  # 28 dBZ

    task = ForecastGenTask(
        forecast=forecast,
        issue_timestamp=t1,
        history=(
            HistoryFrame(timestamp=t0, paths=(stale_base, overlay_t0)),
            HistoryFrame(timestamp=t1, paths=(base_t1, overlay_t1)),
        ),
    )

    overlay_history, base_latest = load_layered_history(task)

    assert [field.timestamp for field in overlay_history] == [t0, t1]
    assert base_latest.timestamp == t1
    # Everything shares one grid so history stacks and the pair stays aligned.
    transforms = {field.transform for field in (*overlay_history, base_latest)}
    assert len(transforms) == 1
    assert base_latest.crs == WEB_MERCATOR
    assert np.nanmax(overlay_history[0].values) == pytest.approx(8.0)
    assert np.nanmax(overlay_history[1].values) == pytest.approx(28.0)
    assert np.nanmax(base_latest.values) == pytest.approx(48.0)


def test_load_layered_history_requires_two_paths_per_frame(tmp_path: Path) -> None:
    forecast = _layered_forecast(tmp_path)
    t1 = datetime(2026, 6, 5, 21, 0)
    task = ForecastGenTask(
        forecast=forecast,
        issue_timestamp=t1,
        history=(HistoryFrame(timestamp=t1, paths=(tmp_path / "only_one.hdf",)),),
    )

    with pytest.raises(ValueError, match="2 input paths"):
        load_layered_history(task)


def test_generate_layered_forecast_fields_warps_pair_with_shared_motion(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}

    def fake_motion_method(method: str):
        def fake_motion(input_images, **kwargs):  # noqa: ANN001, ANN003
            captured["motion_input"] = input_images
            return np.zeros((2, *input_images.shape[1:]), dtype=np.float32)

        return fake_motion

    def fake_extrapolation_method():
        def fake_extrapolate(precip, velocity, timesteps):  # noqa: ANN001
            captured.setdefault("precips", []).append(np.array(precip, copy=True))
            # Identity warp: return the input precip for each lead.
            return np.stack([np.array(precip, copy=True) for _ in timesteps])

        return fake_extrapolate

    monkeypatch.setattr(forecast_generation, "_motion_method", fake_motion_method)
    monkeypatch.setattr(forecast_generation, "_extrapolation_method", fake_extrapolation_method)

    t0 = datetime(2026, 6, 5, 21, 0)
    t1 = t0 + timedelta(minutes=5)
    # Motion history is the denser MAX-Z; its tip doubles as the overlay field.
    base_values = np.array([[20.0, -np.inf], [-np.inf, 20.0]])
    overlay_values = np.array([[40.0, 36.0], [-np.inf, 20.0]])
    overlay_history = [
        _field(np.array([[40.0, -np.inf], [-np.inf, 20.0]]), t0),
        _field(overlay_values, t1),
    ]
    base_latest = _field(base_values, t1)

    generated = generate_layered_forecast_fields(
        overlay_history,
        base_latest,
        minutes=(10,),
        fast_warp=False,
        fast_motion=False,
        fast_idw=False,
    )

    assert sorted(generated) == [10]
    assert len(captured["precips"]) == 2  # base then overlay, one shared velocity
    frame = generated[10]
    assert isinstance(frame, LayeredFrame)
    # Both leads stay physical: no sentinel/banded values anywhere.
    assert np.array_equal(frame.base.values, base_values.astype(np.float32))
    assert np.array_equal(frame.overlay.values, overlay_values.astype(np.float32))
    assert frame.timestamp == t1 + timedelta(minutes=10)
    assert frame.base.timestamp == frame.overlay.timestamp == t1 + timedelta(minutes=10)


def test_generate_layered_forecast_fields_validates_timestamps() -> None:
    t0 = datetime(2026, 6, 5, 21, 0)
    t1 = t0 + timedelta(minutes=5)
    overlay_history = [_field(np.zeros((2, 2)), t0), _field(np.zeros((2, 2)), t1)]
    stale_base = _field(np.zeros((2, 2)), t0)

    with pytest.raises(ValueError, match="share a timestamp"):
        generate_layered_forecast_fields(overlay_history, stale_base, minutes=(10,))


def test_generate_layered_forecast_fields_transparent_when_both_empty(monkeypatch) -> None:  # noqa: ANN001
    def fail_motion_method(method: str):  # noqa: ANN001
        raise AssertionError("motion should not run for empty latest fields")

    monkeypatch.setattr(forecast_generation, "_motion_method", fail_motion_method)

    t0 = datetime(2026, 6, 5, 21, 0)
    t1 = t0 + timedelta(minutes=5)
    overlay_history = [
        _field(np.full((2, 2), 30.0), t0),
        _field(np.full((2, 2), np.nan), t1),
    ]
    base_latest = _field(np.full((2, 2), np.nan), t1)

    generated = generate_layered_forecast_fields(overlay_history, base_latest, minutes=(10,))

    assert sorted(generated) == [10]
    assert isinstance(generated[10], RadarField)
    assert np.isnan(generated[10].values).all()


def test_coarse_motion_matches_full_resolution() -> None:
    """The coarsened motion grid yields a full-resolution, near-identical forecast."""

    pytest.importorskip("pysteps")
    pytest.importorskip("cv2")

    rng = np.random.default_rng(0)
    height, width = 96, 120
    yy, xx = np.mgrid[0:height, 0:width]
    noise = rng.normal(0.0, 1.5, size=(height, width)).astype(np.float32)
    base = datetime(2026, 6, 5, 21, 0)
    fields = [
        _field(
            40.0 * np.exp(-(((xx - (30 + 6 * i)) ** 2 + (yy - (40 + 3 * i)) ** 2) / (2 * 8.0**2))) + noise,
            base + timedelta(minutes=5 * i),
        )
        for i in range(3)
    ]

    full = generate_forecast_fields(fields, minutes=(10, 20, 30), motion_grid_step=1)
    coarse = generate_forecast_fields(fields, minutes=(10, 20, 30), motion_grid_step=2)

    assert sorted(full) == sorted(coarse) == [10, 20, 30]
    for minute in full:
        assert coarse[minute].values.shape == (height, width)
        a, b = full[minute].values, coarse[minute].values
        mask = np.isfinite(a) & np.isfinite(b)
        rmse = float(np.sqrt(np.mean((a[mask] - b[mask]) ** 2)))
        assert rmse < 3.0, f"minute {minute}: coarse motion diverged (RMSE {rmse:.2f})"


def test_fast_idw_matches_pysteps() -> None:
    """The parallel kd-tree IDW must match pysteps to floating-point precision."""

    pysteps_utils = pytest.importorskip("pysteps.utils")
    from radar_server import forecast_fast

    rng = np.random.default_rng(1)
    xy = rng.uniform(0, 200, size=(60, 2))
    uv = rng.normal(0, 5, size=(60, 2))
    xgrid = np.arange(0, 200, 4)
    ygrid = np.arange(0, 160, 4)

    reference = pysteps_utils.get_method("idwinterp2d")(xy, uv, xgrid, ygrid)
    fast = forecast_fast.idw_interpolate(xy, uv, xgrid, ygrid)

    assert fast.shape == reference.shape
    assert np.allclose(fast, reference, atol=1e-6)


def test_fast_motion_matches_pysteps() -> None:
    """The MaskedArray-free sparse Lucas-Kanade path must match pysteps exactly."""

    pytest.importorskip("pysteps")
    pytest.importorskip("cv2")
    from radar_server import forecast_fast
    from radar_server.forecast_generation import _DECL_SCALE, _coarse_motion_tools, _motion_method

    rng = np.random.default_rng(3)
    height, width = 110, 130
    yy, xx = np.mgrid[0:height, 0:width]
    frames = []
    for i in range(3):
        field = 45.0 * np.exp(-(((xx - (40 + 5 * i)) ** 2 + (yy - (55 + 4 * i)) ** 2) / (2 * 10.0**2)))
        field += 25.0 * np.exp(-(((xx - (90 - 4 * i)) ** 2 + (yy - (30 + 2 * i)) ** 2) / (2 * 7.0**2)))
        field = field + rng.normal(0.0, 1.0, size=(height, width))
        # No-data border so the masked path is exercised (mostly-no-data radar).
        field[:8, :] = np.nan
        field[-8:, :] = np.nan
        field[:, :8] = np.nan
        field[:, -8:] = np.nan
        frames.append(field.astype(np.float32))
    stack = np.stack(frames)

    oflow = _motion_method("lucaskanade")
    xy_ref, uv_ref = oflow(np.ma.masked_invalid(stack), dense=False, verbose=False)
    xy_fast, uv_fast = forecast_fast.lk_sparse_vectors(stack)

    assert xy_fast.shape == xy_ref.shape, "fast path found a different number of vectors"

    idwinterp2d, decluster = _coarse_motion_tools()
    xgrid = np.arange(width)
    ygrid = np.arange(height)

    def densify(xy: np.ndarray, uv: np.ndarray) -> np.ndarray:
        xy, uv = decluster(xy, uv, _DECL_SCALE, 1, False)
        return np.asarray(idwinterp2d(xy, uv, xgrid, ygrid), dtype=np.float64)

    rmse = float(np.sqrt(np.mean((densify(xy_ref, uv_ref) - densify(xy_fast, uv_fast)) ** 2)))
    assert rmse < 1e-3, f"fast motion diverged from pysteps (dense velocity RMSE {rmse:.4f})"


def test_fast_warp_matches_pysteps() -> None:
    """The cv2.remap semi-Lagrangian warp must closely match scipy/pysteps."""

    extrapolation = pytest.importorskip("pysteps.extrapolation")
    pytest.importorskip("cv2")
    from radar_server import forecast_fast

    rng = np.random.default_rng(2)
    height, width = 90, 110
    precip = rng.normal(20.0, 5.0, size=(height, width)).astype(np.float32)
    velocity = np.stack(
        [
            np.full((height, width), 1.5, dtype=np.float32),
            np.full((height, width), -0.8, dtype=np.float32),
        ]
    )
    lead_steps = [1.0, 2.0, 3.0]

    reference = extrapolation.get_method("semilagrangian")(precip, velocity, lead_steps)
    fast = forecast_fast.extrapolate(precip, velocity, lead_steps)

    assert fast.shape == reference.shape
    mask = np.isfinite(reference) & np.isfinite(fast)
    rmse = float(np.sqrt(np.mean((reference[mask] - fast[mask]) ** 2)))
    assert rmse < 0.5, f"cv2 warp diverged from scipy (RMSE {rmse:.3f})"


def test_fast_warp_coarse_grid_matches_full_resolution() -> None:
    """Coarsening the warp's trajectory integration stays close to full resolution."""

    pytest.importorskip("cv2")
    from radar_server import forecast_fast

    rng = np.random.default_rng(4)
    height, width = 160, 200
    precip = rng.normal(20.0, 5.0, size=(height, width)).astype(np.float32)
    # A smoothly varying (rotational) velocity field, like a real motion field.
    yy, xx = np.mgrid[0:height, 0:width]
    u = (0.02 * (yy - height / 2)).astype(np.float32)
    v = (-0.02 * (xx - width / 2)).astype(np.float32)
    velocity = np.stack([u, v])
    lead_steps = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]

    full = forecast_fast.extrapolate(precip, velocity, lead_steps, grid_step=1)
    coarse = forecast_fast.extrapolate(precip, velocity, lead_steps, grid_step=2)

    assert coarse.shape == full.shape == (len(lead_steps), height, width)
    mask = np.isfinite(full) & np.isfinite(coarse)
    rmse = float(np.sqrt(np.mean((full[mask] - coarse[mask]) ** 2)))
    assert rmse < 0.5, f"coarse-grid warp diverged from full resolution (RMSE {rmse:.3f})"


def test_coarse_motion_falls_back_on_tiny_grid(monkeypatch) -> None:  # noqa: ANN001
    """A grid too small to coarsen must fall back to the full dense method."""

    calls: dict = {}

    def fake_motion_method(method: str):
        def fake_motion(input_images, **kwargs):  # noqa: ANN001, ANN003
            calls["dense"] = kwargs.get("dense", True)
            return np.zeros((2, *input_images.shape[1:]), dtype=np.float32)

        return fake_motion

    monkeypatch.setattr(forecast_generation, "_motion_method", fake_motion_method)

    motion_stack = np.zeros((3, 2, 2), dtype=np.float32)
    forecast_generation._compute_motion("lucaskanade", motion_stack, grid_step=2)

    assert calls["dense"] is True
