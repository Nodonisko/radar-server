"""Real-data tests against EUMETNET OPERA composite crops, with PNG snapshots.

Fixtures (``fixtures/opera_*.h5``) are two overlapping regional crops of one OPERA
maximum-reflectivity composite (LAEA projection, real precipitation) — see
``fixtures/build_fixtures.py`` to regenerate. They exercise the real non-Mercator
reprojection and the composite/merge paths.

Snapshot testing: each render is compared pixel-wise against a committed reference
PNG in ``snapshots/``. To (re)generate references after an intentional change:

    UPDATE_SNAPSHOTS=1 python -m pytest radar_server/rendering/tests/test_real_data.py
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from radar_server.rendering.core import WEB_MERCATOR
from radar_server.rendering.decode import load_odim_hdf
from radar_server.rendering.palettes import STANDARD_DBZH
from radar_server.rendering.pipeline import render_composite_png, render_radar_png
from radar_server.rendering.reproject import lonlat_bounds, to_web_mercator

_FIXTURES = Path(__file__).resolve().parent / "fixtures"
_SNAPSHOTS = Path(__file__).resolve().parent / "snapshots"
WEST = _FIXTURES / "opera_west.h5"
EAST = _FIXTURES / "opera_east.h5"

pytestmark = pytest.mark.skipif(not WEST.exists(), reason="OPERA fixtures not present")


def _assert_snapshot(name: str, png_path: Path, max_frac_diff: float = 0.005) -> None:
    """Compare a rendered PNG against a committed reference, pixel-wise.

    Creates the reference on first run (and when UPDATE_SNAPSHOTS is set);
    otherwise fails if more than ``max_frac_diff`` of pixels differ, writing a
    ``<name>.actual.png`` alongside the reference for inspection.
    """
    _SNAPSHOTS.mkdir(exist_ok=True)
    ref = _SNAPSHOTS / f"{name}.png"
    if os.environ.get("UPDATE_SNAPSHOTS") or not ref.exists():
        shutil.copyfile(png_path, ref)
        if not os.environ.get("UPDATE_SNAPSHOTS"):
            pytest.skip(f"snapshot {name!r} created; rerun to compare")
        return

    actual = np.asarray(Image.open(png_path).convert("RGBA"))
    expected = np.asarray(Image.open(ref).convert("RGBA"))
    assert actual.shape == expected.shape, f"{name}: size {actual.shape} != snapshot {expected.shape}"
    frac = float(np.any(actual != expected, axis=-1).mean())
    if frac > max_frac_diff:
        dump = _SNAPSHOTS / f"{name}.actual.png"
        shutil.copyfile(png_path, dump)
        raise AssertionError(f"{name}: {frac:.4%} of pixels differ (> {max_frac_diff:.2%}); wrote {dump.name}")


# --- decode / reproject on real LAEA data -------------------------------------

def test_decode_opera_laea() -> None:
    field = load_odim_hdf(WEST)
    assert "laea" in field.crs
    assert field.quantity == "DBZH"
    assert field.shape == (560, 450)
    assert field.timestamp == datetime(2026, 6, 5, 0, 0)
    assert np.isfinite(field.values).any()  # real echoes present


def test_reproject_from_laea_oversamples() -> None:
    field = load_odim_hdf(WEST)
    warped = to_web_mercator(field)
    assert warped.crs == WEB_MERCATOR
    # LAEA -> Mercator is a genuine warp; lossless adds rows for the sec(lat) stretch.
    assert warped.shape[0] > field.shape[0]


# --- standard path ------------------------------------------------------------

def test_render_standard_snapshot(tmp_path: Path) -> None:
    result = render_radar_png(WEST, tmp_path, STANDARD_DBZH, base="west", optimize=False)
    assert result.variants["overlay"].exists()
    _assert_snapshot("standard_west_overlay", result.variants["overlay"])
    _assert_snapshot("standard_west_small", result.variants["overlay_small"])


# --- composite: union (no bounds) ---------------------------------------------

def test_composite_union_snapshot(tmp_path: Path) -> None:
    result = render_composite_png([WEST, EAST], tmp_path, STANDARD_DBZH, base="union", optimize=False)
    wb = lonlat_bounds(to_web_mercator(load_odim_hdf(WEST)))
    eb = lonlat_bounds(to_web_mercator(load_odim_hdf(EAST)))
    assert result.bounds[0] == pytest.approx(min(wb[0], eb[0]), abs=0.05)  # west edge
    assert result.bounds[2] == pytest.approx(max(wb[2], eb[2]), abs=0.05)  # east edge
    _assert_snapshot("composite_union_overlay", result.variants["overlay"])


# --- composite: custom bounds -------------------------------------------------

def test_composite_bounds_snapshot(tmp_path: Path) -> None:
    bounds = (11.0, 46.5, 13.5, 49.0)  # inside the overlap band, covered by both tiles
    full = render_composite_png([WEST, EAST], tmp_path / "full", STANDARD_DBZH, base="u", optimize=False)
    result = render_composite_png([WEST, EAST], tmp_path / "crop", STANDARD_DBZH, base="cropped", bounds=bounds, optimize=False)
    for got, want in zip(result.bounds, bounds):
        assert got == pytest.approx(want, abs=0.05)
    with Image.open(result.variants["overlay"]) as crop, Image.open(full.variants["overlay"]) as whole:
        assert crop.width < whole.width and crop.height < whole.height
    _assert_snapshot("composite_bounds_overlay", result.variants["overlay"])


# --- composite: overlap merge is order-independent (fmax) ----------------------

def test_composite_order_independent(tmp_path: Path) -> None:
    ab = render_composite_png([WEST, EAST], tmp_path / "ab", STANDARD_DBZH, base="ab", optimize=False)
    ba = render_composite_png([EAST, WEST], tmp_path / "ba", STANDARD_DBZH, base="ba", optimize=False)
    a = np.asarray(Image.open(ab.variants["overlay"]).convert("RGBA"))
    b = np.asarray(Image.open(ba.variants["overlay"]).convert("RGBA"))
    assert np.array_equal(a, b)
