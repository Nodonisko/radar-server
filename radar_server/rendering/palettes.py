"""Palette library — the one piece of rendering config shared across countries.

Each :class:`PaletteSpec` carries its colors *and* the value breakpoints they
map to, so a palette is self-contained (the old code split colors into config
and the dBZ breakpoints into the renderer). Which palette a country/product
uses is country configuration, decided elsewhere; this module only defines the
reusable specs.
"""

from __future__ import annotations

from .core import PaletteSpec

# Reflectivity, 4 dBZ steps from 4 to 64 dBZ. Below 4 dBZ is transparent.
STANDARD_DBZH = PaletteSpec(
    name="dbzh",
    quantity="DBZH",
    levels=tuple(float(v) for v in range(4, 68, 4)),
    colors=(
        "#390071",
        "#3001A9",
        "#0200FB",
        "#076CBC",
        "#00A400",
        "#00BB03",
        "#36D700",
        "#9CDD07",
        "#E0DC01",
        "#FBB200",
        "#F78600",
        "#FF5400",
        "#FE0100",
        "#A40003",
        "#FCFCFC",
    ),
)

# Extended reflectivity, adds -12..4 dBZ in lighter tones for very light returns.
EXTENDED_DBZH = PaletteSpec(
    name="dbzh_extended",
    quantity="DBZH",
    levels=tuple(float(v) for v in range(-12, 68, 4)),
    colors=(
        "#E0D4EC",  # -12 to -8 dBZ
        "#C8B8D8",  # -8 to -4 dBZ
        "#B0A0C8",  # -4 to 0 dBZ
        "#8B68A8",  # 0 to 4 dBZ (very light returns)
        "#390071",
        "#3001A9",
        "#0200FB",
        "#076CBC",
        "#00A400",
        "#00BB03",
        "#36D700",
        "#9CDD07",
        "#E0DC01",
        "#FBB200",
        "#F78600",
        "#FF5400",
        "#FE0100",
        "#A40003",
        "#FCFCFC",
    ),
)

# Lookup by name, for config that selects palettes by string.
PALETTES = {p.name: p for p in (STANDARD_DBZH, EXTENDED_DBZH)}
