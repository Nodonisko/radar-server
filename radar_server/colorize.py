"""Map physical values to palette indices via a step lookup.

Replaces matplotlib's ListedColormap/BoundaryNorm with a vectorized
``searchsorted``: deterministic, exact grid-sized output, and it produces an
indexed image directly (no lossy 16-color quantize pass needed downstream).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import numpy as np

from .core import PaletteSpec, RadarField


@dataclass(frozen=True)
class IndexedImage:
    """An indexed (palette) image plus the index reserved for transparency."""

    indices: np.ndarray  # (H, W) uint8
    palette: List[Tuple[int, int, int]]
    transparent_index: int


def colorize(field: RadarField, palette: PaletteSpec) -> IndexedImage:
    if field.quantity != palette.quantity:
        raise ValueError(
            f"palette {palette.name!r} is for {palette.quantity}, field is {field.quantity}"
        )

    levels = np.asarray(palette.levels, dtype=np.float64)
    ncolors = len(palette.colors)
    values = field.values

    # bin index: colors[i] covers [levels[i], levels[i+1]); clamp top, mark below.
    idx = np.searchsorted(levels, values, side="right") - 1
    transparent = np.isnan(values) | (idx < 0)
    np.clip(idx, 0, ncolors - 1, out=idx)

    transparent_index = ncolors
    indices = idx.astype(np.uint8)
    indices[transparent] = transparent_index

    return IndexedImage(
        indices=indices,
        palette=palette.rgb,
        transparent_index=transparent_index,
    )
