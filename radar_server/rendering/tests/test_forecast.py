from __future__ import annotations

import json
from datetime import datetime

import numpy as np

from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField
from radar_server.rendering.forecast import render_forecast_field
from radar_server.rendering.palettes import STANDARD_DBZH


def _field(values: np.ndarray, timestamp: datetime) -> RadarField:
    height, width = values.shape
    return RadarField(
        values=values.astype(np.float32),
        crs=WEB_MERCATOR,
        transform=GeoTransform(
            x_min=0.0,
            y_max=100.0,
            px=1.0,
            py=1.0,
            width=width,
            height=height,
        ),
        quantity="DBZH",
        timestamp=timestamp,
    )


def test_render_forecast_field_writes_variants_and_sidecar(tmp_path) -> None:  # noqa: ANN001
    timestamp = datetime(2026, 6, 5, 21, 10)
    field = _field(np.array([[10.0, np.nan], [35.0, 52.0]]), timestamp)
    ready_paths = []

    result = render_forecast_field(
        field,
        tmp_path,
        STANDARD_DBZH,
        base="radar_test_20260605_2100_fct10",
        minute=10,
        variants=(("overlay", 1.0),),
        optimize=False,
        on_output_ready=ready_paths.append,
    )

    assert result.base == "radar_test_20260605_2100_fct10"
    overlay = tmp_path / "radar_test_20260605_2100_fct10_overlay.png"
    sidecar = tmp_path / "radar_test_20260605_2100_fct10.json"
    assert overlay.exists()
    assert sidecar.exists()
    manifest = json.loads(sidecar.read_text())
    assert manifest["timestamp"] == timestamp.isoformat()
    assert manifest["sources"] == ["forecast_10m"]
    assert "overlay" in manifest["variants"]
    assert ready_paths == [overlay]
