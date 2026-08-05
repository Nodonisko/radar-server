"""Layered composite: solid base field plus faded overlay-only echoes.

Used for Czechia CAPPI (ground-level rain) under MAX-Z (column-max echo that
CAPPI does not show). Both inputs are warped onto one Web Mercator grid, then
merged into a single float field with two value bands so the ordinary
downsample → colorize → encode tail can emit one PNG:

  - base echoes keep their physical dBZ (solid palette colors)
  - overlay-only echoes are shifted into a disjoint lower band and colorized
    with the same RGB steps at reduced opacity

Input order is significant: ``hdf_paths[0]`` is the solid base, ``hdf_paths[1]``
is the faded overlay.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np

from .composite import _target_grid
from .core import WEB_MERCATOR, PaletteSpec, RadarField, Rgba
from .decode import load_odim_hdf
from .pipeline import (
    DEFAULT_VARIANTS,
    Bounds,
    OutputReadyCallback,
    RenderResult,
    _EmitTimings,
    _emit,
    _log_render_performance,
)
from .reproject import resample_to_grid

LOGGER = logging.getLogger(__name__)

# Shift overlay-only dBZ into a band well below any base palette floor so the
# two ranges never overlap. 1000 keeps ~15 four-dBZ steps comfortably clear of
# the solid band starting at 4 dBZ.
OVERLAY_VALUE_OFFSET = 1000.0

# "Reduced opacity by 60%" relative to the solid base colors.
OVERLAY_OPACITY = 0.4


def build_layered_palette(
    base: PaletteSpec,
    *,
    overlay_opacity: float = OVERLAY_OPACITY,
    offset: float = OVERLAY_VALUE_OFFSET,
) -> PaletteSpec:
    """Build a two-band palette: faded overlay colors, then solid base colors.

    Overlay bins use ``level - offset``. The final overlay level is the base
    floor, leaving a dead gap bin that encoded values never land in.
    """
    if not 0.0 <= overlay_opacity <= 1.0:
        raise ValueError(f"overlay_opacity must be in [0.0, 1.0], got {overlay_opacity}")
    if offset <= 0:
        raise ValueError(f"offset must be positive, got {offset}")

    overlay_levels = tuple(level - offset for level in base.levels[:-1])
    levels = overlay_levels + base.levels
    colors = base.colors + base.colors
    alphas = tuple(overlay_opacity for _ in base.colors) + tuple(1.0 for _ in base.colors)
    return PaletteSpec(
        name=base.name,
        quantity=base.quantity,
        levels=levels,
        colors=colors,
        alphas=alphas,
    )


def merge_layered_fields(
    base: RadarField,
    overlay: RadarField,
    *,
    floor: float,
    offset: float = OVERLAY_VALUE_OFFSET,
) -> RadarField:
    """Merge ``base`` with overlay-only echoes shifted by ``-offset``.

    A cell is overlay-only when the overlay is at/above ``floor`` and the base
    is not (clear sky, below floor, or NaN). Where both have echo, the base
    value wins so ground-level rain stays solid.
    """
    if base.shape != overlay.shape:
        raise ValueError(f"layered fields must share a grid, got {base.shape} vs {overlay.shape}")
    if base.crs != overlay.crs or base.transform != overlay.transform:
        raise ValueError("layered fields must share CRS and geotransform")
    if base.quantity != overlay.quantity:
        raise ValueError(
            f"layered fields must share a quantity, got {base.quantity!r} vs {overlay.quantity!r}"
        )
    if base.timestamp != overlay.timestamp:
        raise ValueError(
            f"layered fields must share a timestamp, got {base.timestamp} vs {overlay.timestamp}"
        )

    base_values = base.values
    overlay_values = overlay.values
    base_echo = np.isfinite(base_values) & (base_values >= floor)
    overlay_echo = np.isfinite(overlay_values) & (overlay_values >= floor)
    only_overlay = overlay_echo & ~base_echo

    # Start from the base field, but force non-echo / non-nodata base cells to
    # -inf. Leaving below-floor dBZ in place would land them in the dead gap
    # between the overlay and solid palette bands and paint them as faded.
    merged = np.array(base_values, dtype=np.float32, copy=True)
    merged[~base_echo & ~np.isnan(merged)] = -np.inf
    merged[only_overlay] = overlay_values[only_overlay] - np.float32(offset)
    return RadarField(
        values=merged,
        crs=base.crs,
        transform=base.transform,
        quantity=base.quantity,
        timestamp=base.timestamp,
    )


def render_layered_png(
    hdf_paths: Iterable[Path],
    output_dir: Path,
    palette: PaletteSpec,
    *,
    base: str,
    bounds: Bounds | None = None,
    variants: Sequence[tuple[str, float]] = DEFAULT_VARIANTS,
    optimize: bool = True,
    nodata_fill: Rgba | None = None,
    on_output_ready: OutputReadyCallback | None = None,
    overlay_opacity: float = OVERLAY_OPACITY,
) -> RenderResult:
    """Render base+overlay HDF5 inputs as one layered Web Mercator PNG."""
    paths = list(hdf_paths)
    if len(paths) != 2:
        raise ValueError(f"layered render needs exactly 2 inputs (base, overlay), got {len(paths)}")

    total_start = time.perf_counter()

    step_start = time.perf_counter()
    source_fields = [load_odim_hdf(path, quantity=palette.quantity) for path in paths]
    decode_time = time.perf_counter() - step_start

    step_start = time.perf_counter()
    timestamps = {field.timestamp for field in source_fields}
    if len(timestamps) > 1:
        raise ValueError(f"layered inputs must share a timestamp, got {sorted(timestamps)}")

    # One shared target grid so base/overlay stay pixel-aligned after warp/crop.
    target = _target_grid(source_fields, bounds)
    timestamp = next(iter(timestamps))
    warped = [
        RadarField(
            values=resample_to_grid(source, target),
            crs=WEB_MERCATOR,
            transform=target,
            quantity=source.quantity,
            timestamp=timestamp,
        )
        for source in source_fields
    ]
    field = merge_layered_fields(warped[0], warped[1], floor=palette.levels[0])
    layered_palette = build_layered_palette(palette, overlay_opacity=overlay_opacity)
    transform_time = time.perf_counter() - step_start

    emit_timings = _EmitTimings()
    result = _emit(
        field,
        output_dir,
        layered_palette,
        base,
        variants,
        optimize,
        sources=[path.name for path in paths],
        timings=emit_timings,
        metadata_bounds=bounds,
        nodata_fill=nodata_fill,
        on_output_ready=on_output_ready,
    )
    _log_render_performance(
        base=base,
        source_count=len(paths),
        result=result,
        field=field,
        total=time.perf_counter() - total_start,
        decode=decode_time,
        transform_label="layered",
        transform=transform_time,
        emit=emit_timings,
    )
    return result
