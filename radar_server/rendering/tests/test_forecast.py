from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np

import radar_server.rendering.forecast as forecast_module
from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField
from radar_server.rendering.forecast import render_forecast
from radar_server.rendering.palettes import STANDARD_DBZH
from radar_server.rendering.pipeline import RenderResult


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


def test_render_forecast_preserves_nan_mask_and_uses_requested_lead_steps(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    captured = {}
    emitted: list[RadarField] = []

    def fake_motion(input_images, *, verbose=True):  # noqa: ANN001
        captured["motion_input"] = input_images
        captured["verbose"] = verbose
        return np.zeros((2, 2, 2), dtype=np.float32)

    def fake_extrapolate(precip, velocity, timesteps):  # noqa: ANN001
        captured["precip"] = precip.copy()
        captured["velocity"] = velocity
        captured["timesteps"] = tuple(timesteps)
        return np.stack(
            [
                np.full_like(precip, 20.0, dtype=np.float32),
                np.full_like(precip, 30.0, dtype=np.float32),
            ]
        )

    def fake_emit(field, output_dir, palette, base, variants, optimize, sources, timings):  # noqa: ANN001
        emitted.append(field)
        return RenderResult(base=base, variants={}, sidecar=output_dir / f"{base}.json", bounds=(0, 0, 1, 1))

    monkeypatch.setattr(forecast_module.motion, "get_method", lambda method: fake_motion)
    monkeypatch.setattr(forecast_module.nowcasts, "get_method", lambda method: fake_extrapolate)
    monkeypatch.setattr(forecast_module, "_emit", fake_emit)

    timestamp = datetime(2026, 6, 5, 21, 0)
    fields = [
        _field(np.array([[10.0, np.nan], [15.0, 20.0]]), timestamp),
        _field(np.array([[12.0, np.nan], [18.0, 22.0]]), timestamp + timedelta(minutes=7)),
    ]

    results = render_forecast(
        fields=fields,
        output_dir=tmp_path,
        palette=STANDARD_DBZH,
        base="radar_test_20260605_2107",
        forecast_minutes=(10, 20),
        optimize=False,
    )

    assert len(results) == 2
    assert captured["verbose"] is False
    assert np.ma.isMaskedArray(captured["motion_input"])
    assert captured["motion_input"].mask[0, 0, 1]
    assert np.isnan(captured["precip"][0, 1])
    assert captured["timesteps"] == (10 / 7, 20 / 7)
    assert [field.timestamp for field in emitted] == [
        timestamp + timedelta(minutes=17),
        timestamp + timedelta(minutes=27),
    ]
