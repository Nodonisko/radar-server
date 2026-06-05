"""Resolution reduction for smaller output variants.

Max-pooling over the lossless grid: each output cell takes the peak value of the
input cells in its footprint, so a coarser variant loses spatial detail but never
hides an intense cell (a storm core). Averaging would be wrong for reflectivity,
which is logarithmic (dBZ); max also matches the source MAX-Z composite.

``factor`` may be fractional (e.g. 1.5). Windows are placed with
``fmax.reduceat`` so they tile the input exactly (every cell in one window) and
ignore NaN nodata. At an integer factor this is a plain k x k block max.

The reduced grid covers the exact same extent as the input, so every variant of
a frame shares one lat/lon bounding box.
"""

from __future__ import annotations

import numpy as np

from .core import GeoTransform, RadarField


def downsample_max(field: RadarField, factor: float) -> RadarField:
    """Reduce ``field`` by ``factor`` (>=1, may be fractional) via max-pooling."""
    if factor <= 1:
        return field

    values = field.values
    height, width = values.shape
    out_h = max(1, round(height / factor))
    out_w = max(1, round(width / factor))

    # Window start indices, rounded (not floored) so windows centre on the
    # output cells: this keeps the fractional-factor registration error
    # symmetric (<=half a cell) instead of biased one way. Strictly increasing
    # because the step (in/out) exceeds 1; reduceat then pools contiguous,
    # non-overlapping, exhaustive spans. Integer factors are unaffected.
    row_bounds = np.rint(np.arange(out_h) * height / out_h).astype(np.intp)
    col_bounds = np.rint(np.arange(out_w) * width / out_w).astype(np.intp)

    # fmax ignores NaN; an all-NaN window stays NaN (-> transparent).
    pooled = np.fmax.reduceat(values, row_bounds, axis=0)
    pooled = np.fmax.reduceat(pooled, col_bounds, axis=1).astype(np.float32)

    gt = field.transform
    # Keep the original extent so the bbox matches the full-resolution variant.
    new_transform = GeoTransform(
        x_min=gt.x_min,
        y_max=gt.y_max,
        px=gt.px * width / out_w,
        py=gt.py * height / out_h,
        width=out_w,
        height=out_h,
    )
    return RadarField(
        values=pooled,
        crs=field.crs,
        transform=new_transform,
        quantity=field.quantity,
        timestamp=field.timestamp,
    )
