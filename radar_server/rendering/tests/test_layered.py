"""Tests for the CAPPI-under-MAX-Z layered renderer."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
import pytest
from PIL import Image

from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, PaletteSpec, RadarField
from radar_server.rendering.layered import (
    OVERLAY_OPACITY,
    OVERLAY_VALUE_OFFSET,
    build_layered_palette,
    merge_layered_fields,
    render_layered_png,
)
from radar_server.rendering.palettes import STANDARD_DBZH

_MERC = (
    "+proj=merc +lat_ts=0 +lon_0=0 +k=1.0 +x_0=-1254222.15 +y_0=-6702777.85 "
    "+a=6378137.0 +b=6378137.0 +units=m +nadgrids=@null +wktext +no_defs"
)
_UL_LON, _UL_LAT = 11.2669, 51.4584


def _field(values: np.ndarray) -> RadarField:
    height, width = values.shape
    return RadarField(
        values=values.astype(np.float32),
        crs=WEB_MERCATOR,
        transform=GeoTransform(x_min=0.0, y_max=100.0, px=10.0, py=10.0, width=width, height=height),
        quantity="DBZH",
        timestamp=datetime(2026, 8, 5, 16, 40),
    )


def _write_odim(path: Path, raw: np.ndarray, *, product: bytes) -> Path:
    height, width = raw.shape
    with h5py.File(path, "w") as hdf:
        hdf.attrs["Conventions"] = b"ODIM_H5/V2_4"
        what = hdf.create_group("what")
        what.attrs["date"] = b"20260805"
        what.attrs["time"] = b"164000"
        what.attrs["object"] = b"COMP"
        where = hdf.create_group("where")
        where.attrs["projdef"] = _MERC.encode()
        where.attrs["xsize"] = width
        where.attrs["ysize"] = height
        where.attrs["xscale"] = 1555.7
        where.attrs["yscale"] = 1555.7
        where.attrs["UL_lon"] = _UL_LON
        where.attrs["UL_lat"] = _UL_LAT
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


def test_build_layered_palette_has_faded_then_solid_bands() -> None:
    layered = build_layered_palette(STANDARD_DBZH)

    assert layered.name == STANDARD_DBZH.name
    assert layered.quantity == STANDARD_DBZH.quantity
    assert len(layered.colors) == 2 * len(STANDARD_DBZH.colors)
    assert len(layered.levels) == len(layered.colors) + 1
    assert layered.alphas is not None
    assert layered.alphas[: len(STANDARD_DBZH.colors)] == tuple(
        OVERLAY_OPACITY for _ in STANDARD_DBZH.colors
    )
    assert layered.alphas[len(STANDARD_DBZH.colors) :] == tuple(1.0 for _ in STANDARD_DBZH.colors)
    assert layered.levels[0] == pytest.approx(STANDARD_DBZH.levels[0] - OVERLAY_VALUE_OFFSET)
    assert layered.levels[len(STANDARD_DBZH.colors)] == pytest.approx(STANDARD_DBZH.levels[0])


def test_merge_layered_fields_keeps_base_and_shifts_overlay_only() -> None:
    base = _field(
        np.array(
            [
                [-np.inf, 20.0, np.nan],
                [-np.inf, 2.0, 28.0],
            ]
        )
    )
    overlay = _field(
        np.array(
            [
                [40.0, 50.0, np.nan],
                [-np.inf, 36.0, 12.0],
            ]
        )
    )

    merged = merge_layered_fields(base, overlay, floor=4.0)

    assert merged.values[0, 0] == pytest.approx(40.0 - OVERLAY_VALUE_OFFSET)
    assert merged.values[0, 1] == pytest.approx(20.0)  # base wins where both echo
    assert np.isnan(merged.values[0, 2])
    assert np.isneginf(merged.values[1, 0])
    # Base below floor: overlay-only echo is painted faded.
    assert merged.values[1, 1] == pytest.approx(36.0 - OVERLAY_VALUE_OFFSET)
    assert merged.values[1, 2] == pytest.approx(28.0)


def test_merge_clears_below_floor_base_without_overlay() -> None:
    base = _field(np.array([[2.0, -np.inf], [np.nan, 20.0]]))
    overlay = _field(np.array([[-np.inf, -np.inf], [np.nan, 40.0]]))

    merged = merge_layered_fields(base, overlay, floor=4.0)

    # Below-floor base with no overlay echo must become clear-sky, not a gap value.
    assert np.isneginf(merged.values[0, 0])
    assert np.isneginf(merged.values[0, 1])
    assert np.isnan(merged.values[1, 0])
    assert merged.values[1, 1] == pytest.approx(20.0)


def test_render_layered_png_solid_base_and_faded_overlay(tmp_path: Path) -> None:
    # 120 raw -> 28 dBZ. CAPPI paints a left block; MAX-Z extends further right.
    height, width = 12, 16
    cappi_raw = np.zeros((height, width), dtype=np.uint8)
    maxz_raw = np.zeros((height, width), dtype=np.uint8)
    cappi_raw[3:9, 2:7] = 120
    maxz_raw[3:9, 2:12] = 120

    cappi = _write_odim(tmp_path / "cappi.hdf", cappi_raw, product=b"PCAPPI")
    maxz = _write_odim(tmp_path / "maxz.hdf", maxz_raw, product=b"MAX")

    result = render_layered_png(
        [cappi, maxz],
        tmp_path / "out",
        STANDARD_DBZH,
        base="layered",
        optimize=False,
        variants=(("overlay", 1.0),),
    )
    image = Image.open(result.variants["overlay"]).convert("RGBA")
    pixels = np.asarray(image)

    # Shared echo cell is fully opaque with the solid 28 dBZ color.
    solid = STANDARD_DBZH.rgb[6]  # [28, 32) -> index 6
    assert tuple(pixels[5, 4, :3]) == solid
    assert pixels[5, 4, 3] == 255

    # MAX-Z-only cell uses the same RGB at 70% opacity.
    assert tuple(pixels[5, 9, :3]) == solid
    assert pixels[5, 9, 3] == round(OVERLAY_OPACITY * 255)

    # Clear sky stays fully transparent.
    assert pixels[0, 0, 3] == 0


def test_render_layered_png_requires_two_inputs(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly 2 inputs"):
        render_layered_png([], tmp_path, STANDARD_DBZH, base="x", optimize=False)


def test_palette_alphas_validation() -> None:
    with pytest.raises(ValueError, match="alphas"):
        PaletteSpec(
            name="bad",
            quantity="DBZH",
            levels=(0.0, 4.0, 8.0),
            colors=("#000000", "#ffffff"),
            alphas=(0.5,),
        )
