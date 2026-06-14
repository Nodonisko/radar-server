from __future__ import annotations

import json
import struct
from datetime import datetime
from pathlib import Path

import numpy as np

from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField
from radar_server.rendering.forecast import render_forecast_field
from radar_server.rendering.palettes import STANDARD_DBZH


def _png_comment(path: Path) -> str | None:
    blob = path.read_bytes()
    pos = 8
    while pos < len(blob):
        length = struct.unpack(">I", blob[pos : pos + 4])[0]
        chunk_type = blob[pos + 4 : pos + 8]
        data = blob[pos + 8 : pos + 8 + length]
        if chunk_type == b"tEXt" and data.startswith(b"Comment\x00"):
            return data.split(b"\x00", 1)[1].decode("latin-1")
        pos += 12 + length
        if chunk_type == b"IEND":
            break
    return None


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
    bounds = (11.27, 48.04, 19.64, 51.46)

    result = render_forecast_field(
        field,
        tmp_path,
        STANDARD_DBZH,
        base="radar_test_20260605_2100_fct10",
        minute=10,
        variants=(("overlay", 1.0),),
        optimize=False,
        bounds=bounds,
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
    assert manifest["bounds"] == {"west": bounds[0], "south": bounds[1], "east": bounds[2], "north": bounds[3]}
    assert "overlay" in manifest["variants"]
    assert _png_comment(overlay) == "GeoBox=11.27,48.04,19.64,51.46"
    assert ready_paths == [overlay]
