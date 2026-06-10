from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np

from radar_server import forecast_store
from radar_server.config import ForecastProduct, GeoBounds, ProductConfig, cz_maxz, timestamped_base
from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField

ISSUE = datetime(2026, 6, 5, 21, 0)


def _parent(tmp_path: Path) -> ProductConfig:
    return ProductConfig(
        id="test",
        label="Test",
        inputs=(cz_maxz,),
        output_dir=tmp_path / "out",
        geo_bounds=GeoBounds(0, 0, 0, 0),
        base_name=timestamped_base("radar_test"),
    )


def _forecast(tmp_path: Path, *, minutes: tuple[int, ...] = (10, 20)) -> ForecastProduct:
    return ForecastProduct(
        id="test_forecast",
        parent=_parent(tmp_path),
        minutes=minutes,
        field_dir=tmp_path / "fields",
    )


def _field(values: np.ndarray, timestamp: datetime) -> RadarField:
    height, width = values.shape
    return RadarField(
        values=values.astype(np.float32),
        crs=WEB_MERCATOR,
        transform=GeoTransform(x_min=0.0, y_max=100.0, px=1.0, py=1.0, width=width, height=height),
        quantity="DBZH",
        timestamp=timestamp,
    )


def test_save_and_load_field_round_trips_everything(tmp_path: Path) -> None:
    values = np.array([[10.0, np.nan], [15.0, 20.0]])
    field = _field(values, datetime(2026, 6, 5, 21, 10))
    path = tmp_path / "fields" / "radar_test_20260605_2100_fct10.npz"

    forecast_store.save_field(field, path, issue_timestamp=ISSUE, minute=10)
    stored = forecast_store.load_field(path)

    np.testing.assert_array_equal(stored.field.values, values.astype(np.float32))
    assert stored.field.transform == field.transform
    assert stored.field.crs == WEB_MERCATOR
    assert stored.field.quantity == "DBZH"
    assert stored.field.timestamp == field.timestamp
    assert stored.issue_timestamp == ISSUE
    assert stored.minute == 10
    assert not list(path.parent.glob("*.part"))  # atomic write left no temp file


def test_write_forecast_fields_clears_superseded_issues(tmp_path: Path) -> None:
    forecast = _forecast(tmp_path)
    values = np.zeros((2, 2))
    old_issue = datetime(2026, 6, 5, 20, 55)
    forecast_store.write_forecast_fields(
        forecast,
        old_issue,
        {minute: _field(values, old_issue) for minute in forecast.minutes},
    )
    assert len(list(forecast.field_dir.glob("*.npz"))) == 2

    new_paths = forecast_store.write_forecast_fields(
        forecast,
        ISSUE,
        {minute: _field(values, ISSUE) for minute in forecast.minutes},
    )

    remaining = sorted(forecast.field_dir.glob("*.npz"))
    assert remaining == sorted(new_paths.values())
    for path in remaining:
        issue_timestamp, _minute = forecast_store.read_field_metadata(path)
        assert issue_timestamp == ISSUE


def test_existing_field_paths_reports_only_present_minutes(tmp_path: Path) -> None:
    forecast = _forecast(tmp_path)
    forecast_store.save_field(
        _field(np.zeros((2, 2)), ISSUE),
        forecast_store.field_path(forecast, ISSUE, 10),
        issue_timestamp=ISSUE,
        minute=10,
    )

    existing = forecast_store.existing_field_paths(forecast, ISSUE)

    assert set(existing) == {10}


def test_discover_forecast_render_units_finds_fields_without_outputs(tmp_path: Path) -> None:
    forecast = _forecast(tmp_path)
    forecast_store.write_forecast_fields(
        forecast,
        ISSUE,
        {minute: _field(np.zeros((2, 2)), ISSUE) for minute in forecast.minutes},
    )

    # Render outputs for minute 10 exist; only minute 20 should be discovered.
    base_10 = forecast_store.forecast_base(forecast, ISSUE, 10)
    for path in forecast_store.frame_output_paths(forecast, base_10):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"exists")

    units = forecast_store.discover_forecast_render_units(forecast)

    assert [unit.minute for unit in units] == [20]
    unit = units[0]
    assert unit.issue_timestamp == ISSUE
    assert unit.field_path == forecast_store.field_path(forecast, ISSUE, 20)
    assert unit.base == forecast_store.forecast_base(forecast, ISSUE, 20)


def test_discover_skips_unreadable_field_files(tmp_path: Path) -> None:
    forecast = _forecast(tmp_path)
    forecast.field_dir.mkdir(parents=True)
    (forecast.field_dir / "corrupt_fct10.npz").write_bytes(b"not an npz")

    assert forecast_store.discover_forecast_render_units(forecast) == []


def test_prune_forecast_fields_deletes_old_and_unreadable(tmp_path: Path) -> None:
    forecast = _forecast(tmp_path)
    old_issue = datetime(2026, 6, 5, 18, 0)
    recent_issue = datetime(2026, 6, 5, 20, 0)
    old_path = forecast_store.field_path(forecast, old_issue, 10)
    recent_path = forecast_store.field_path(forecast, recent_issue, 10)
    forecast_store.save_field(_field(np.zeros((2, 2)), old_issue), old_path, issue_timestamp=old_issue, minute=10)
    forecast_store.save_field(
        _field(np.zeros((2, 2)), recent_issue), recent_path, issue_timestamp=recent_issue, minute=10
    )
    corrupt = forecast.field_dir / "corrupt_fct10.npz"
    corrupt.write_bytes(b"not an npz")

    deleted = forecast_store.prune_forecast_fields([forecast], now=datetime(2026, 6, 5, 21, 5))

    assert set(deleted) == {old_path, corrupt}
    assert not old_path.exists()
    assert recent_path.exists()


def test_expected_forecast_paths_covers_all_minutes_and_variants(tmp_path: Path) -> None:
    forecast = _forecast(tmp_path)

    paths = forecast_store.expected_forecast_paths(forecast, ISSUE)

    # 2 minutes x (1 sidecar + 2 variants)
    assert len(paths) == 6
    assert all(path.parent == forecast.output_dir for path in paths)
    names = {path.name for path in paths}
    assert "radar_test_20260605_2100_fct10.json" in names
    assert "radar_test_20260605_2100_fct20_overlay.png" in names
