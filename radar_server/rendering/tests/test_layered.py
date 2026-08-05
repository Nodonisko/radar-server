"""Tests for the CAPPI-under-MAX-Z layered renderer."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import h5py
import numpy as np
import pytest
from PIL import Image

from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, RadarField, Rgba
from radar_server.rendering.layered import (
    OVERLAY_OPACITY,
    LayeredFrame,
    colorize_layered,
    render_layered_png,
)
from radar_server.rendering.palettes import STANDARD_DBZH

_MERC = (
    "+proj=merc +lat_ts=0 +lon_0=0 +k=1.0 +x_0=-1254222.15 +y_0=-6702777.85 "
    "+a=6378137.0 +b=6378137.0 +units=m +nadgrids=@null +wktext +no_defs"
)
_UL_LON, _UL_LAT = 11.2669, 51.4584

_NCOLORS = len(STANDARD_DBZH.colors)
_TRANSPARENT = 2 * _NCOLORS


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


def test_colorize_layered_solid_base_faded_overlay_transparent_rest() -> None:
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

    image = colorize_layered(base, overlay, STANDARD_DBZH)

    # Index layout: [0, n) solid base, [n, 2n) faded overlay, 2n transparent.
    assert image.indices[0, 0] == _NCOLORS + 9  # overlay-only 40 dBZ, faded
    assert image.indices[0, 1] == 4  # both echo -> solid base 20 dBZ
    assert image.indices[0, 2] == _TRANSPARENT  # both missing, no nodata fill
    assert image.indices[1, 0] == _TRANSPARENT  # both clear sky
    assert image.indices[1, 1] == _NCOLORS + 8  # base below floor -> faded overlay 36 dBZ
    assert image.indices[1, 2] == 6  # solid base 28 dBZ (overlay below it is irrelevant)

    assert image.transparent_index == _TRANSPARENT
    assert image.palette == STANDARD_DBZH.rgb + STANDARD_DBZH.rgb
    assert image.palette_alphas == (255,) * _NCOLORS + (round(OVERLAY_OPACITY * 255),) * _NCOLORS
    assert image.nodata_index is None


def test_colorize_layered_nodata_follows_base_unless_overlay_echoes() -> None:
    base = _field(np.array([[np.nan, np.nan], [2.0, 20.0]]))
    overlay = _field(np.array([[np.nan, 40.0], [-np.inf, -np.inf]]))

    image = colorize_layered(base, overlay, STANDARD_DBZH, nodata_fill=Rgba(0, 0, 0, 0.15))

    assert image.nodata_index == _TRANSPARENT + 1
    assert image.indices[0, 0] == image.nodata_index  # both missing -> nodata
    assert image.indices[0, 1] == _NCOLORS + 9  # overlay echo overrides missing base
    assert image.indices[1, 0] == _TRANSPARENT  # below-floor base is clear sky, not nodata
    assert image.indices[1, 1] == 4  # solid base
    assert image.nodata_rgba == (0, 0, 0, 38)


def test_layered_frame_validates_pair_alignment() -> None:
    base = _field(np.zeros((2, 2)))
    overlay = _field(np.zeros((2, 2)))

    stale = RadarField(
        values=overlay.values,
        crs=overlay.crs,
        transform=overlay.transform,
        quantity=overlay.quantity,
        timestamp=overlay.timestamp + timedelta(minutes=5),
    )
    with pytest.raises(ValueError, match="timestamp"):
        LayeredFrame(base=base, overlay=stale)

    with pytest.raises(ValueError, match="quantity"):
        LayeredFrame(
            base=base,
            overlay=RadarField(
                values=overlay.values,
                crs=overlay.crs,
                transform=overlay.transform,
                quantity="TH",
                timestamp=overlay.timestamp,
            ),
        )

    with pytest.raises(ValueError, match="overlay_opacity"):
        LayeredFrame(base=base, overlay=overlay, overlay_opacity=1.5)


def test_layered_frame_downsampled_keeps_base_priority() -> None:
    # One pooling window holds a weak base echo and a stronger overlay-only
    # echo; the documented rule is that the base echo still renders solid.
    base = _field(np.array([[20.0, np.nan], [np.nan, np.nan]]))
    overlay = _field(np.array([[np.nan, 60.0], [np.nan, np.nan]]))
    frame = LayeredFrame(base=base, overlay=overlay)

    small = frame.downsampled(2.0)

    assert small.base.shape == (1, 1)
    image = small.colorized(STANDARD_DBZH)
    assert image.indices[0, 0] == 4  # solid base 20 dBZ, not the faded 60 dBZ


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

    # MAX-Z-only cell uses the same RGB at the overlay opacity.
    assert tuple(pixels[5, 9, :3]) == solid
    assert pixels[5, 9, 3] == round(OVERLAY_OPACITY * 255)

    # Clear sky stays fully transparent.
    assert pixels[0, 0, 3] == 0


def test_render_layered_png_requires_two_inputs(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly 2 inputs"):
        render_layered_png([], tmp_path, STANDARD_DBZH, base="x", optimize=False)
