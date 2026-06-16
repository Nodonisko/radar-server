from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

import radar_server.forecast_generation as forecast_generation
from radar_server.config import ForecastProduct, GeoBounds, ProductConfig, cz_maxz, timestamped_base
from radar_server.forecast_generation import generate_for_task, generate_forecast_fields
from radar_server.queueing import ForecastGenTask, HistoryFrame
from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField


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

    def fake_generate(fields, *, minutes, method, floor_level, motion_grid_step):  # noqa: ANN001
        captured["fields"] = fields
        captured["minutes"] = minutes
        captured["generation_method"] = method
        captured["floor_level"] = floor_level
        captured["motion_grid_step"] = motion_grid_step
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


def test_coarse_motion_falls_back_on_tiny_grid(monkeypatch) -> None:  # noqa: ANN001
    """A grid too small to coarsen must fall back to the full dense method."""

    calls: dict = {}

    def fake_motion_method(method: str):
        def fake_motion(input_images, **kwargs):  # noqa: ANN001, ANN003
            calls["dense"] = kwargs.get("dense", True)
            return np.zeros((2, *input_images.shape[1:]), dtype=np.float32)

        return fake_motion

    monkeypatch.setattr(forecast_generation, "_motion_method", fake_motion_method)

    motion_input = np.ma.masked_invalid(np.zeros((3, 2, 2), dtype=np.float32))
    forecast_generation._compute_motion("lucaskanade", motion_input, grid_step=2)

    assert calls["dense"] is True
