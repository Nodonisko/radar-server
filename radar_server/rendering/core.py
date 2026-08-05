"""Core value types shared across the rendering chain.

These types are country-agnostic. A decoder produces a :class:`RadarField`,
the reprojection step rewrites it into a common CRS, and a :class:`PaletteSpec`
drives colorization. Nothing here knows about a specific country or source.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

import numpy as np

# Common output CRS. Every field is reprojected to this before colorizing so a
# single web map (Leaflet/Mapbox/Google, all Web Mercator) can place any
# country's overlay by lat/lon bounding box.
WEB_MERCATOR = "EPSG:3857"
WGS84 = "EPSG:4326"


@dataclass(frozen=True)
class GeoTransform:
    """Maps pixel (col, row) to projected (x, y) for an axis-aligned grid.

    ``x_min``/``y_max`` are the outer edges of the top-left pixel. ``px``/``py``
    are positive pixel sizes in projected units; y decreases as row increases
    (row 0 is the northern edge).
    """

    x_min: float
    y_max: float
    px: float
    py: float
    width: int
    height: int

    @property
    def x_max(self) -> float:
        return self.x_min + self.width * self.px

    @property
    def y_min(self) -> float:
        return self.y_max - self.height * self.py

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        """(x_min, y_min, x_max, y_max) in projected units."""
        return (self.x_min, self.y_min, self.x_max, self.y_max)

    def col_centers(self) -> np.ndarray:
        return self.x_min + (np.arange(self.width) + 0.5) * self.px

    def row_centers(self) -> np.ndarray:
        return self.y_max - (np.arange(self.height) + 0.5) * self.py


@dataclass(frozen=True)
class RadarField:
    """A decoded radar grid plus its georeferencing.

    ``values`` is a 2D float array in physical units of ``quantity`` (e.g. dBZ
    for ``DBZH``). NaN marks missing data; below-threshold/clear-sky cells carry
    a value below the palette's first level so they render transparent.
    """

    values: np.ndarray
    crs: str
    transform: GeoTransform
    quantity: str
    timestamp: datetime

    @property
    def shape(self) -> Tuple[int, int]:
        return self.values.shape


def _parse_hex(color: str) -> Tuple[int, int, int]:
    h = color.lstrip("#")
    if len(h) != 6:
        raise ValueError(f"expected #RRGGBB color, got {color!r}")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


@dataclass(frozen=True)
class Rgba:
    """A solid color plus an alpha, used for special-cell fills (e.g. nodata).

    ``r``/``g``/``b`` are 0-255 channels and ``a`` is opacity in ``[0.0, 1.0]``
    (matching CSS ``rgba(...)``). Stored separately from :class:`PaletteSpec`
    colors because palette entries are always opaque; only the fill for
    missing-data cells carries partial transparency.
    """

    r: int
    g: int
    b: int
    a: float = 1.0

    def __post_init__(self) -> None:
        for name, channel in (("r", self.r), ("g", self.g), ("b", self.b)):
            if not 0 <= channel <= 255:
                raise ValueError(f"Rgba {name} must be 0-255, got {channel}")
        if not 0.0 <= self.a <= 1.0:
            raise ValueError(f"Rgba alpha must be in [0.0, 1.0], got {self.a}")

    @property
    def rgb(self) -> Tuple[int, int, int]:
        return (self.r, self.g, self.b)

    @property
    def alpha_byte(self) -> int:
        """Alpha as an 8-bit value for the PNG ``tRNS`` chunk."""
        return round(self.a * 255)


@dataclass(frozen=True)
class PaletteSpec:
    """A step colormap bound to a physical quantity.

    ``levels`` are the value boundaries (ascending) and must have exactly one
    more entry than ``colors``: ``colors[i]`` fills ``[levels[i], levels[i+1])``.
    Values below ``levels[0]`` are transparent; values at/above ``levels[-1]``
    clamp to the top color.
    """

    name: str
    quantity: str
    levels: Tuple[float, ...]
    colors: Tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.levels) != len(self.colors) + 1:
            raise ValueError(
                f"palette {self.name!r}: levels ({len(self.levels)}) must be "
                f"colors ({len(self.colors)}) + 1"
            )
        # Strictly ascending: equal adjacent levels would form a zero-width bin
        # whose color no value can ever land in (a dead palette entry).
        if any(b <= a for a, b in zip(self.levels, self.levels[1:])):
            raise ValueError(f"palette {self.name!r}: levels must be strictly ascending")

    @property
    def rgb(self) -> list[Tuple[int, int, int]]:
        return [_parse_hex(c) for c in self.colors]
