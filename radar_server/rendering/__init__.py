"""Rendering pipeline: radar HDF5 → reprojected, colorized PNG(s).

Public entry points: :func:`render_radar_png`, :func:`render_composite_png`,
:func:`render_batch`. See the submodules for the chain
(decode → reproject → downsample → colorize → encode).
"""

from .palettes import EXTENDED_DBZH, PALETTES, STANDARD_DBZH
from .pipeline import RenderResult, render_batch, render_composite_png, render_radar_png

__all__ = [
    "render_radar_png",
    "render_composite_png",
    "render_batch",
    "RenderResult",
    "STANDARD_DBZH",
    "EXTENDED_DBZH",
    "PALETTES",
]
