"""Reproject a :class:`RadarField` into Web Mercator (EPSG:3857).

This is the step that makes multi-country support correct. National composites
ship in different projections (Web Mercator, Lambert, polar stereographic, ...);
web maps are all Web Mercator. We warp every field to 3857 so a single lat/lon
bounding box places the overlay correctly anywhere.

The warp is nearest-neighbor, which is the right choice for radar values (no
interpolation across reflectivity classes). When the source is already Mercator
on the WGS84 sphere (e.g. CHMI), the warp is effectively a translation and
reproduces the input grid pixel-for-pixel.
"""

from __future__ import annotations

from typing import Tuple

import numpy as np
from pyproj import Transformer

from .core import WEB_MERCATOR, WGS84, GeoTransform, RadarField

# Points sampled per source-bbox edge when measuring the target extent. Edges
# curve under reprojection, so corners alone can underestimate the bbox.
_EDGE_SAMPLES = 64

# Lattice density (per axis) for measuring the finest projected cell size.
_CELL_SAMPLES = 128


def _target_bbox(field: RadarField, fwd: Transformer) -> Tuple[float, float, float, float]:
    gt = field.transform
    xs = np.linspace(gt.x_min, gt.x_max, _EDGE_SAMPLES)
    ys = np.linspace(gt.y_min, gt.y_max, _EDGE_SAMPLES)
    # Perimeter of the source grid in source projected coords.
    px = np.concatenate([xs, xs, np.full_like(ys, gt.x_min), np.full_like(ys, gt.x_max)])
    py = np.concatenate([np.full_like(xs, gt.y_min), np.full_like(xs, gt.y_max), ys, ys])
    mx, my = fwd.transform(px, py)
    return float(mx.min()), float(my.min()), float(mx.max()), float(my.max())


def _min_projected_cell(field: RadarField, fwd: Transformer) -> float:
    """Finest source-cell size (metres) once projected into Web Mercator.

    Measures one source pixel (east and south neighbours) over a lattice and
    returns the smallest edge. The minimum sits at the equatorward edge, where
    Mercator's ``sec(lat)`` stretch is weakest; sampling there is what makes the
    output lossless for non-Mercator sources.
    """
    gt = field.transform
    cols = np.linspace(0, gt.width - 1, min(_CELL_SAMPLES, gt.width))
    rows = np.linspace(0, gt.height - 1, min(_CELL_SAMPLES, gt.height))
    cc, rr = (a.ravel() for a in np.meshgrid(cols, rows))

    x = gt.x_min + (cc + 0.5) * gt.px
    y = gt.y_max - (rr + 0.5) * gt.py
    mx, my = fwd.transform(x, y)
    mxe, mye = fwd.transform(x + gt.px, y)  # one pixel east
    mxs, mys = fwd.transform(x, y - gt.py)  # one pixel south

    dx = np.hypot(mxe - mx, mye - my)
    dy = np.hypot(mxs - mx, mys - my)
    return float(min(dx.min(), dy.min()))


def finest_cell(field: RadarField, fwd: Transformer | None = None) -> float:
    """Finest source-cell size (metres) of ``field`` projected into Web Mercator.

    Pass ``fwd`` (a field.crs -> Web Mercator transformer) to reuse one across
    several calls on the same field.
    """
    if fwd is None:
        fwd = Transformer.from_crs(field.crs, WEB_MERCATOR, always_xy=True)
    return _min_projected_cell(field, fwd)


def web_mercator_bbox(field: RadarField, fwd: Transformer | None = None) -> Tuple[float, float, float, float]:
    """``field`` extent (x_min, y_min, x_max, y_max) in Web Mercator metres."""
    if fwd is None:
        fwd = Transformer.from_crs(field.crs, WEB_MERCATOR, always_xy=True)
    return _target_bbox(field, fwd)


def resample_to_grid(field: RadarField, target: GeoTransform) -> np.ndarray:
    """Nearest-neighbor sample ``field`` onto a given Web Mercator ``target`` grid.

    Returns a (height, width) array; cells outside the source footprint are NaN.
    This is the shared warp used by both single-frame reprojection and
    compositing (which calls it once per input against a common grid).
    """
    inv = Transformer.from_crs(WEB_MERCATOR, field.crs, always_xy=True)
    gt = field.transform

    xx, yy = np.meshgrid(target.col_centers(), target.row_centers())
    sx, sy = inv.transform(xx.ravel(), yy.ravel())

    # Target cells outside the source projection's domain invert to non-finite
    # coords. Replace them before the int cast: NaN would otherwise cast to 0 (an
    # in-range index) and silently sample the source corner pixel.
    finite = np.isfinite(sx) & np.isfinite(sy)
    col_f = np.where(finite, (sx - gt.x_min) / gt.px - 0.5, -1.0)
    row_f = np.where(finite, (gt.y_max - sy) / gt.py - 0.5, -1.0)
    src_col = np.rint(col_f).astype(np.int64)
    src_row = np.rint(row_f).astype(np.int64)
    inside = (src_col >= 0) & (src_col < gt.width) & (src_row >= 0) & (src_row < gt.height)

    out = np.full(target.height * target.width, np.nan, dtype=np.float32)
    out[inside] = field.values[src_row[inside], src_col[inside]]
    return out.reshape(target.height, target.width)


def to_web_mercator(field: RadarField) -> RadarField:
    """Return ``field`` resampled onto a lossless Web Mercator grid.

    Output pixel size is the finest source cell projected into Mercator, so no
    native cell is ever dropped: the projection's ``sec(lat)`` stretch is
    absorbed by adding pixels where needed. For a source already in Mercator
    (e.g. CHMI) this reproduces the input grid pixel-for-pixel.
    """
    fwd = Transformer.from_crs(field.crs, WEB_MERCATOR, always_xy=True)
    x_min, y_min, x_max, y_max = _target_bbox(field, fwd)
    bbox_w, bbox_h = x_max - x_min, y_max - y_min
    res = _min_projected_cell(field, fwd)

    # Epsilon guards against float noise adding a spurious row/column when the
    # extent divides evenly (e.g. the CHMI identity case).
    width = max(1, int(np.ceil(bbox_w / res - 1e-6)))
    height = max(1, int(np.ceil(bbox_h / res - 1e-6)))

    out_t = GeoTransform(x_min=x_min, y_max=y_max, px=res, py=res, width=width, height=height)
    values = resample_to_grid(field, out_t)
    return RadarField(values=values, crs=WEB_MERCATOR, transform=out_t, quantity=field.quantity, timestamp=field.timestamp)


def lonlat_bounds(field: RadarField) -> Tuple[float, float, float, float]:
    """(west, south, east, north) in degrees, for placing the PNG on a web map.

    Expects a field already in Web Mercator.
    """
    to_wgs = Transformer.from_crs(field.crs, WGS84, always_xy=True)
    x_min, y_min, x_max, y_max = field.transform.bbox
    west, south = to_wgs.transform(x_min, y_min)
    east, north = to_wgs.transform(x_max, y_max)
    return west, south, east, north
