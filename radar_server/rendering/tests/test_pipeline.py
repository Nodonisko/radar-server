"""Pipeline tests.

Self-contained: a small synthetic ODIM HDF5 file is generated on demand, so the
suite no longer depends on external sample data being present.
"""

from __future__ import annotations

import atexit
import json
import logging
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
import pytest
from PIL import Image

from radar_server.rendering.colorize import colorize
from radar_server.rendering.composite import composite_to_web_mercator
from radar_server.rendering.core import WEB_MERCATOR, GeoTransform, PaletteSpec, RadarField
from radar_server.rendering.decode import load_odim_hdf
from radar_server.rendering.downsample import downsample_max
from radar_server.rendering.palettes import EXTENDED_DBZH, STANDARD_DBZH
from radar_server.rendering.pipeline import render_batch, render_composite_png, render_radar_png
from radar_server.rendering.reproject import lonlat_bounds, resample_to_grid, to_web_mercator

# Spherical Mercator (as CHMI ships), so the reproject of the synthetic file is a
# pixel-exact identity, matching the real composites the project consumes.
_MERC = (
    "+proj=merc +lat_ts=0 +lon_0=0 +k=1.0 +x_0=-1254222.15 +y_0=-6702777.85 "
    "+a=6378137.0 +b=6378137.0 +units=m +nadgrids=@null +wktext +no_defs"
)
_UL_LON, _UL_LAT = 11.2669, 51.4584


def _write_synthetic_odim(path: Path) -> Path:
    height, width = 16, 20
    raw = np.zeros((height, width), dtype=np.uint8)  # 0 == undetect (clear sky)
    raw[4:10, 5:15] = 120                            # 120*0.5-32 = 28 dBZ block
    raw[6, 8] = 180                                  # 180*0.5-32 = 58 dBZ peak
    raw[0, 0] = 255                                  # 255 == nodata
    with h5py.File(path, "w") as f:
        f.attrs["Conventions"] = b"ODIM_H5/V2_4"
        what = f.create_group("what")
        what.attrs["date"] = b"20260605"
        what.attrs["time"] = b"144000"
        what.attrs["object"] = b"COMP"
        where = f.create_group("where")
        where.attrs["projdef"] = _MERC.encode()
        where.attrs["xsize"] = width
        where.attrs["ysize"] = height
        where.attrs["xscale"] = 1555.7
        where.attrs["yscale"] = 1555.7
        where.attrs["UL_lon"] = _UL_LON
        where.attrs["UL_lat"] = _UL_LAT
        ds = f.create_group("dataset1")
        ds.create_group("what").attrs["product"] = b"MAX"
        data1 = ds.create_group("data1")
        data1.create_dataset("data", data=raw)
        dwhat = data1.create_group("what")
        dwhat.attrs["gain"] = 0.5
        dwhat.attrs["offset"] = -32.0
        dwhat.attrs["nodata"] = 255.0
        dwhat.attrs["undetect"] = 0.0
        dwhat.attrs["quantity"] = b"DBZH"
    return path


_SAMPLE: Path | None = None


def _sample() -> Path:
    global _SAMPLE
    if _SAMPLE is None:
        tmpdir = Path(tempfile.mkdtemp(prefix="radar_server_test_"))
        atexit.register(shutil.rmtree, tmpdir, ignore_errors=True)
        _SAMPLE = _write_synthetic_odim(tmpdir / "synthetic.hdf")
    return _SAMPLE


def _grid(values: np.ndarray, px: float = 10.0) -> RadarField:
    h, w = values.shape
    gt = GeoTransform(x_min=0.0, y_max=100.0, px=px, py=px, width=w, height=h)
    return RadarField(values=values, crs=WEB_MERCATOR, transform=gt, quantity="DBZH", timestamp=datetime(2026, 1, 1))


# --- decode / palette ---------------------------------------------------------

def test_palette_validation() -> None:
    # wrong level/color count
    with pytest.raises(ValueError):
        PaletteSpec(name="bad", quantity="DBZH", levels=(0.0, 1.0), colors=("#000000", "#ffffff"))
    # equal adjacent levels -> zero-width bin (dead color)
    with pytest.raises(ValueError):
        PaletteSpec(name="dup", quantity="DBZH", levels=(0.0, 4.0, 4.0), colors=("#000000", "#ffffff"))
    # the shipped palettes are valid
    assert STANDARD_DBZH.rgb and EXTENDED_DBZH.rgb


def test_decode_reads_projection() -> None:
    field = load_odim_hdf(_sample())
    assert field.quantity == "DBZH"
    assert field.crs.startswith("+proj=")
    assert field.values.shape == (16, 20)
    assert field.transform.width == 20 and field.transform.height == 16
    assert field.timestamp == datetime(2026, 6, 5, 14, 40)


def test_decode_raises_for_missing_quantity() -> None:
    # The file carries only DBZH; requesting another quantity must fail loudly
    # rather than silently mislabel a different dataset (M1).
    with pytest.raises(ValueError):
        load_odim_hdf(_sample(), quantity="RATE")


# --- reproject ----------------------------------------------------------------

def test_reproject_is_web_mercator_and_preserves_shape() -> None:
    field = load_odim_hdf(_sample())
    warped = to_web_mercator(field)
    assert warped.crs == WEB_MERCATOR
    assert warped.shape == field.shape  # Mercator source -> identity


def test_reproject_bounds_anchor_to_ul_corner() -> None:
    warped = to_web_mercator(load_odim_hdf(_sample()))
    west, south, east, north = lonlat_bounds(warped)
    assert west == pytest.approx(_UL_LON, abs=0.02)
    assert north == pytest.approx(_UL_LAT, abs=0.02)
    assert east > west and north > south


def test_lossless_reproject_adds_resolution_for_non_mercator_source() -> None:
    # Plate-carree (EPSG:4326) over 40-60N: Mercator stretches it north-south, so
    # the lossless warp adds rows to preserve the fine (southern) edge.
    gt = GeoTransform(x_min=0.0, y_max=60.0, px=0.1, py=0.1, width=100, height=200)
    field = RadarField(
        values=np.zeros((200, 100), dtype=np.float32),
        crs="EPSG:4326",
        transform=gt,
        quantity="DBZH",
        timestamp=datetime(2026, 1, 1),
    )
    warped = to_web_mercator(field)
    assert warped.shape[1] == pytest.approx(100, abs=2)
    assert warped.shape[0] > field.shape[0]


def test_resample_masks_out_of_domain_cells() -> None:
    # Orthographic source: the inverse is undefined off the visible hemisphere.
    # Out-of-domain target cells must become NaN, not sample the corner pixel (M2).
    src = RadarField(
        values=np.full((20, 20), 30.0, dtype=np.float32),
        crs="+proj=ortho +lat_0=0 +lon_0=0 +R=6371000 +units=m +no_defs",
        transform=GeoTransform(x_min=-5e5, y_max=5e5, px=5e4, py=5e4, width=20, height=20),
        quantity="DBZH",
        timestamp=datetime(2026, 1, 1),
    )
    src.values[0, 0] = 7.0  # sentinel the NaN->0 bug would smear across the globe
    target = GeoTransform(x_min=-2e7, y_max=2e7, px=1e6, py=1e6, width=40, height=40)

    out = resample_to_grid(src, target)
    finite = int(np.isfinite(out).sum())
    assert finite > 0                      # the small footprint near (0,0) is sampled
    assert finite < out.size * 0.5         # the rest of the globe is masked, not corner-filled


# --- colorize -----------------------------------------------------------------

def test_colorize_marks_transparent_and_clamps() -> None:
    values = np.array([[np.nan, 0.0, 6.0, 100.0]], dtype=np.float32)
    field = RadarField(values, WEB_MERCATOR, GeoTransform(0.0, 0.0, 1.0, 1.0, 4, 1), "DBZH", datetime(2026, 1, 1))
    image = colorize(field, STANDARD_DBZH)
    ti = image.transparent_index
    assert ti == len(STANDARD_DBZH.colors)
    # NaN and below-floor (0 < 4) transparent; 6 -> first bin; 100 -> top bin.
    assert image.indices.tolist() == [[ti, ti, 0, len(STANDARD_DBZH.colors) - 1]]


# --- downsample ---------------------------------------------------------------

def test_downsample_integer_keeps_peak_and_preserves_extent() -> None:
    values = np.array(
        [
            [0.0, 0.0, 10.0, 0.0],
            [0.0, 0.0, 0.0, 45.0],          # 45 dBZ core in the top-right block
            [5.0, 0.0, np.nan, np.nan],
            [0.0, 0.0, np.nan, np.nan],     # bottom-right block all nodata
        ],
        dtype=np.float32,
    )
    field = _grid(values)
    out = downsample_max(field, 2)

    assert out.shape == (2, 2)
    assert out.values[0, 1] == 45.0            # peak preserved, not averaged away
    assert out.values[0, 0] == 0.0
    assert out.values[1, 0] == 5.0
    assert np.isnan(out.values[1, 1])          # all-nodata block stays transparent
    assert out.transform.px == 20.0 and out.transform.py == 20.0
    assert out.transform.x_max == field.transform.x_max
    assert out.transform.y_min == field.transform.y_min


def test_downsample_fractional_factor() -> None:
    values = np.zeros((6, 6), dtype=np.float32)
    values[4, 4] = 50.0
    field = _grid(values)

    out = downsample_max(field, 1.5)
    assert out.shape == (4, 4)                  # round(6 / 1.5) = 4
    assert out.values.max() == 50.0             # peak survives fractional pooling
    assert out.transform.x_max == field.transform.x_max


# --- render -------------------------------------------------------------------

def test_render_writes_variants_and_sidecar(tmp_path: Path) -> None:
    result = render_radar_png(_sample(), tmp_path, STANDARD_DBZH, base="frame", optimize=False)

    assert result.base == "frame"
    assert set(result.variants) == {"overlay", "overlay_small"}
    for path in result.variants.values():
        assert path.exists()
        with Image.open(path) as img:
            assert img.mode == "P"
            assert "transparency" in img.info

    with Image.open(result.variants["overlay"]) as full, Image.open(result.variants["overlay_small"]) as small:
        assert small.width < full.width and small.height < full.height

    payload = json.loads(result.sidecar.read_text())
    assert payload["palette"] == "dbzh"
    assert set(payload["variants"]) == {"overlay", "overlay_small"}
    assert set(payload["bounds"]) == {"west", "south", "east", "north"}


def test_render_logs_single_performance_summary(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)

    render_radar_png(_sample(), tmp_path, STANDARD_DBZH, base="logged_frame", optimize=False)

    pipeline_logs = [
        record.getMessage()
        for record in caplog.records
        if record.name == "radar_server.rendering.pipeline" and record.levelno == logging.INFO
    ]
    encode_info_logs = [
        record
        for record in caplog.records
        if record.name == "radar_server.rendering.encode" and record.levelno >= logging.INFO
    ]

    assert len(pipeline_logs) == 1
    assert "Rendered logged_frame in " in pipeline_logs[0]
    assert "decode=" in pipeline_logs[0]
    assert "reproject=" in pipeline_logs[0]
    assert "emit=" in pipeline_logs[0]
    assert "downsample=" in pipeline_logs[0]
    assert "colorize=" in pipeline_logs[0]
    assert "encode=" in pipeline_logs[0]
    assert "png_save=" in pipeline_logs[0]
    assert "oxipng=" in pipeline_logs[0]
    assert "sidecar=" not in pipeline_logs[0]
    assert encode_info_logs == []


def test_extended_palette_renders(tmp_path: Path) -> None:
    result = render_radar_png(_sample(), tmp_path, EXTENDED_DBZH, base="frame_extended", optimize=False)
    assert result.base == "frame_extended"
    assert all(p.exists() for p in result.variants.values())


def test_render_batch_skips_failures(tmp_path: Path) -> None:
    # One good file + one unreadable file: the batch returns the success and does
    # not abort (M3).
    bad = tmp_path / "bad.hdf"
    bad.write_bytes(b"not an hdf file")
    results = render_batch(
        [(_sample(), "good"), (bad, "bad")], tmp_path / "out", STANDARD_DBZH, optimize=False
    )
    assert len(results) == 1
    assert results[0].base == "good"
    assert results[0].variants["overlay"].exists()


def test_render_batch_rejects_duplicate_bases(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        render_batch([(_sample(), "dup"), (_sample(), "dup")], tmp_path, STANDARD_DBZH, optimize=False)


# --- composite ----------------------------------------------------------------

def test_composite_overlap_takes_max() -> None:
    a = _grid(np.array([[10.0, np.nan], [np.nan, 5.0]], dtype=np.float32))
    b = _grid(np.array([[3.0, 7.0], [np.nan, 8.0]], dtype=np.float32))

    out = composite_to_web_mercator([a, b])
    assert out.crs == WEB_MERCATOR
    assert out.shape == (2, 2)
    assert out.values[0, 0] == 10.0      # max(10, 3)
    assert out.values[0, 1] == 7.0       # max(nodata, 7)
    assert np.isnan(out.values[1, 0])    # both nodata
    assert out.values[1, 1] == 8.0       # max(5, 8)


def test_composite_requires_shared_timestamp() -> None:
    a = _grid(np.zeros((2, 2), dtype=np.float32))  # timestamp 2026-01-01
    gt = GeoTransform(x_min=0.0, y_max=100.0, px=10.0, py=10.0, width=2, height=2)
    b = RadarField(values=np.zeros((2, 2), dtype=np.float32), crs=WEB_MERCATOR, transform=gt,
                   quantity="DBZH", timestamp=datetime(2026, 1, 2))

    with pytest.raises(ValueError):
        composite_to_web_mercator([a, b])


def test_composite_default_bounds_is_union(tmp_path: Path) -> None:
    sample = _sample()
    full = render_radar_png(sample, tmp_path / "full", STANDARD_DBZH, base="single", optimize=False)
    comp = render_composite_png([sample], tmp_path / "comp", STANDARD_DBZH, base="union", optimize=False)
    for cb, fb in zip(comp.bounds, full.bounds):
        assert cb == pytest.approx(fb, abs=0.05)


def test_composite_crops_to_custom_bounds(tmp_path: Path) -> None:
    sample = _sample()
    full = render_radar_png(sample, tmp_path / "full", STANDARD_DBZH, base="single", optimize=False)
    fw, fs, fe, fn = full.bounds
    dx, dy = (fe - fw) * 0.25, (fn - fs) * 0.25
    bounds = (fw + dx, fs + dy, fe - dx, fn - dy)  # sub-box strictly inside

    comp = render_composite_png([sample], tmp_path / "comp", STANDARD_DBZH, base="cz_box", bounds=bounds, optimize=False)

    assert comp.base == "cz_box"
    assert (tmp_path / "comp" / "cz_box_overlay.png").exists()
    for cb, rb in zip(comp.bounds, bounds):
        assert cb == pytest.approx(rb, abs=0.05)

    with Image.open(comp.variants["overlay"]) as cropped, Image.open(full.variants["overlay"]) as whole:
        assert cropped.width < whole.width

    payload = json.loads(comp.sidecar.read_text())
    assert payload["sources"] == [sample.name]
