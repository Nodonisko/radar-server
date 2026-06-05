"""Merge multiple radar fields into one Web Mercator composite.

Strategy: build a single target grid in 3857, then warp every input into it and
combine with a NaN-aware max. Because all inputs sample the exact same cell
centres there are no mosaic seams, and overlap resolves to the strongest echo
(consistent with the reflectivity-max used elsewhere).

  - resolution = finest cell across all inputs (lossless for the densest radar)
  - extent     = custom lat/lon bounds if given, else the union of all inputs
  - inputs must share a timestamp (caller groups contemporaneous scans)
"""

from __future__ import annotations

from typing import Optional, Sequence, Tuple

import numpy as np
from pyproj import Transformer

from .core import WEB_MERCATOR, WGS84, GeoTransform, RadarField
from .reproject import finest_cell, resample_to_grid, web_mercator_bbox


def _target_grid(
    fields: Sequence[RadarField],
    bounds: Optional[Tuple[float, float, float, float]],
) -> GeoTransform:
    # One forward transformer per field, reused for cell size and (when needed)
    # the extent, instead of rebuilding it for each.
    fwds = [Transformer.from_crs(f.crs, WEB_MERCATOR, always_xy=True) for f in fields]
    res = min(finest_cell(f, fwd) for f, fwd in zip(fields, fwds))

    if bounds is not None:
        west, south, east, north = bounds
        to_merc = Transformer.from_crs(WGS84, WEB_MERCATOR, always_xy=True)
        xs, ys = to_merc.transform((west, east), (south, north))
        x_min, x_max = min(xs), max(xs)
        y_min, y_max = min(ys), max(ys)
    else:
        boxes = [web_mercator_bbox(f, fwd) for f, fwd in zip(fields, fwds)]
        x_min = min(b[0] for b in boxes)
        y_min = min(b[1] for b in boxes)
        x_max = max(b[2] for b in boxes)
        y_max = max(b[3] for b in boxes)

    # Epsilon matches to_web_mercator: avoids a spurious trailing row/column.
    width = max(1, int(np.ceil((x_max - x_min) / res - 1e-6)))
    height = max(1, int(np.ceil((y_max - y_min) / res - 1e-6)))
    return GeoTransform(x_min=x_min, y_max=y_max, px=res, py=res, width=width, height=height)


def composite_to_web_mercator(
    fields: Sequence[RadarField],
    *,
    bounds: Optional[Tuple[float, float, float, float]] = None,
) -> RadarField:
    """Composite ``fields`` onto one lossless Web Mercator grid via max overlap.

    ``bounds`` is ``(west, south, east, north)`` in WGS84 degrees; ``None`` uses
    the union of all input extents.
    """
    if not fields:
        raise ValueError("composite needs at least one field")

    timestamps = {f.timestamp for f in fields}
    if len(timestamps) > 1:
        raise ValueError(f"composite inputs must share a timestamp, got {sorted(timestamps)}")

    target = _target_grid(fields, bounds)

    accumulator = np.full((target.height, target.width), np.nan, dtype=np.float32)
    for field in fields:
        # fmax: real echoes win over NaN; overlapping echoes resolve to the max.
        accumulator = np.fmax(accumulator, resample_to_grid(field, target))

    return RadarField(
        values=accumulator,
        crs=WEB_MERCATOR,
        transform=target,
        quantity=fields[0].quantity,
        timestamp=next(iter(timestamps)),
    )
