"""Map physical values to palette indices via a step lookup.

Replaces matplotlib's ListedColormap/BoundaryNorm with a vectorized
``searchsorted``: deterministic, exact grid-sized output, and it produces an
indexed image directly (no lossy 16-color quantize pass needed downstream).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np

from .core import PaletteSpec, RadarField, Rgba


@dataclass(frozen=True)
class IndexedImage:
    """An indexed (palette) image plus the index reserved for transparency.

    ``transparent_index`` is always fully transparent (clear-sky / below-floor
    cells). ``nodata_index``/``nodata_rgba`` are populated only when missing-data
    (NaN) cells should render with a partially transparent fill instead of being
    fully transparent; when ``nodata_index`` is ``None`` NaN cells fold into
    ``transparent_index``.
    """

    indices: np.ndarray  # (H, W) uint8
    palette: List[Tuple[int, int, int]]
    transparent_index: int
    nodata_index: Optional[int] = None
    nodata_rgba: Optional[Tuple[int, int, int, int]] = None


def colorize(
    field: RadarField,
    palette: PaletteSpec,
    *,
    nodata_fill: Rgba | None = None,
) -> IndexedImage:
    if field.quantity != palette.quantity:
        raise ValueError(
            f"palette {palette.name!r} is for {palette.quantity}, field is {field.quantity}"
        )

    levels = np.asarray(palette.levels, dtype=np.float64)
    ncolors = len(palette.colors)
    values = field.values

    # bin index: colors[i] covers [levels[i], levels[i+1]); clamp top, mark below.
    idx = np.searchsorted(levels, values, side="right") - 1
    nodata = np.isnan(values)
    below_floor = idx < 0  # clear-sky (-inf) and below-threshold valid cells
    np.clip(idx, 0, ncolors - 1, out=idx)

    transparent_index = ncolors
    indices = idx.astype(np.uint8)

    if nodata_fill is None:
        # Default: missing data and below-floor both fully transparent.
        indices[nodata | below_floor] = transparent_index
        return IndexedImage(
            indices=indices,
            palette=palette.rgb,
            transparent_index=transparent_index,
        )

    # Missing-data cells get their own partially transparent fill; clear-sky and
    # below-floor cells stay fully transparent.
    nodata_index = ncolors + 1
    indices[below_floor] = transparent_index
    indices[nodata] = nodata_index
    return IndexedImage(
        indices=indices,
        palette=palette.rgb,
        transparent_index=transparent_index,
        nodata_index=nodata_index,
        nodata_rgba=(nodata_fill.r, nodata_fill.g, nodata_fill.b, nodata_fill.alpha_byte),
    )
