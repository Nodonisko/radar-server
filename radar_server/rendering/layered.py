"""Layered composite: solid base field plus faded overlay-only echoes.

Used for Czechia pseudoCAPPI (ground-level rain) under MAX-Z (column-max echo
that CAPPI does not show). Both inputs are warped onto one shared Web Mercator
grid and carried as a :class:`LayeredFrame` — two ordinary physical-dBZ fields —
all the way to colorization, where they are combined at the palette-index
level:

  - base echoes take the solid palette colors,
  - overlay-only echoes take the same RGB colors at reduced opacity,
  - where both have echo the base wins, so ground-level rain stays solid.

No merged sentinel values exist anywhere; stored and in-flight fields stay
physical. Input order is significant: ``hdf_paths[0]`` is the solid base,
``hdf_paths[1]`` is the faded overlay.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np

from .colorize import IndexedImage, colorize
from .composite import target_grid
from .core import WEB_MERCATOR, GeoTransform, PaletteSpec, RadarField, Rgba
from .decode import load_odim_hdf
from .downsample import downsample_max
from .pipeline import (
    DEFAULT_VARIANTS,
    Bounds,
    EmitTimings,
    OutputReadyCallback,
    RenderResult,
    emit_frame,
    log_render_performance,
)
from .reproject import resample_to_grid

LOGGER = logging.getLogger(__name__)

# RenderPipeline.id for products that emit CAPPI-under-MAX-Z layered PNGs.
# Forecast generation/rendering keys off this to keep motion/warp in sync.
LAYERED_PIPELINE_ID = "layered_png"

# Opacity of overlay-only echoes relative to the solid base colors.
OVERLAY_OPACITY = 0.4


def is_layered_pipeline(pipeline_id: str) -> bool:
    return pipeline_id == LAYERED_PIPELINE_ID


def decode_layered_pair(paths: Sequence[Path], *, quantity: str) -> tuple[RadarField, RadarField]:
    """Decode and validate one ``(base, overlay)`` HDF pair."""
    if len(paths) != 2:
        raise ValueError(f"layered render needs exactly 2 inputs (base, overlay), got {len(paths)}")
    base, overlay = (load_odim_hdf(path, quantity=quantity) for path in paths)
    if base.timestamp != overlay.timestamp:
        raise ValueError(
            f"layered inputs must share a timestamp, got {base.timestamp} vs {overlay.timestamp}"
        )
    return base, overlay


@dataclass(frozen=True)
class LayeredFrame:
    """A pixel-aligned ``(base, overlay)`` pair rendered as one layered PNG.

    Both fields stay in physical units; the faded-overlay presentation is
    applied only when the frame is colorized. Satisfies the emit tail's
    ``ComposedFrame`` protocol, so it can flow through the ordinary
    downsample -> colorize -> encode chain and the forecast field store.
    """

    base: RadarField
    overlay: RadarField
    overlay_opacity: float = OVERLAY_OPACITY

    def __post_init__(self) -> None:
        base, overlay = self.base, self.overlay
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
        if not 0.0 <= self.overlay_opacity <= 1.0:
            raise ValueError(f"overlay_opacity must be in [0.0, 1.0], got {self.overlay_opacity}")

    @property
    def crs(self) -> str:
        return self.base.crs

    @property
    def transform(self) -> GeoTransform:
        return self.base.transform

    @property
    def quantity(self) -> str:
        return self.base.quantity

    @property
    def timestamp(self) -> datetime:
        return self.base.timestamp

    def downsampled(self, factor: float) -> "LayeredFrame":
        """Max-pool base and overlay separately onto the coarser grid.

        Reduced-resolution cells keep the per-cell rule: any base echo within
        a pooling window renders solid, even when the window also contains a
        stronger overlay-only echo. That is deliberate — ground-level rain is
        never hidden behind a faded column-max echo.
        """
        if factor <= 1:
            return self
        return LayeredFrame(
            base=downsample_max(self.base, factor),
            overlay=downsample_max(self.overlay, factor),
            overlay_opacity=self.overlay_opacity,
        )

    def colorized(self, palette: PaletteSpec, *, nodata_fill: Rgba | None = None) -> IndexedImage:
        return colorize_layered(
            self.base,
            self.overlay,
            palette,
            overlay_opacity=self.overlay_opacity,
            nodata_fill=nodata_fill,
        )


def colorize_layered(
    base: RadarField,
    overlay: RadarField,
    palette: PaletteSpec,
    *,
    overlay_opacity: float = OVERLAY_OPACITY,
    nodata_fill: Rgba | None = None,
) -> IndexedImage:
    """Combine base and overlay into one indexed image at the palette level.

    Index layout: ``[0, n)`` solid base colors, ``[n, 2n)`` faded overlay
    colors (same RGB at ``overlay_opacity``), then the transparent slot and —
    with ``nodata_fill`` — the nodata slot. Rules, per cell:

      - base echo wins and renders solid,
      - otherwise an overlay echo renders faded,
      - otherwise missing base data renders as nodata,
      - otherwise the cell is transparent (clear sky / below floor).
    """
    if not 0.0 <= overlay_opacity <= 1.0:
        raise ValueError(f"overlay_opacity must be in [0.0, 1.0], got {overlay_opacity}")
    ncolors = len(palette.colors)
    if 2 * ncolors + 2 > 256:
        raise ValueError(
            f"palette {palette.name!r} has too many colors ({ncolors}) for a layered 8-bit image"
        )

    base_image = colorize(base, palette, nodata_fill=nodata_fill)
    overlay_image = colorize(overlay, palette)

    base_indices = base_image.indices
    overlay_indices = overlay_image.indices
    base_echo = base_indices < ncolors
    overlay_echo = overlay_indices < ncolors

    transparent_index = 2 * ncolors
    indices = np.full(base_indices.shape, transparent_index, dtype=np.uint8)
    nodata_index = None
    if base_image.nodata_index is not None:
        nodata_index = transparent_index + 1
        indices[base_indices == base_image.nodata_index] = nodata_index
    indices[overlay_echo] = overlay_indices[overlay_echo] + ncolors
    indices[base_echo] = base_indices[base_echo]

    return IndexedImage(
        indices=indices,
        palette=base_image.palette + base_image.palette,
        transparent_index=transparent_index,
        nodata_index=nodata_index,
        nodata_rgba=base_image.nodata_rgba,
        palette_alphas=(255,) * ncolors + (round(overlay_opacity * 255),) * ncolors,
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
) -> RenderResult:
    """Render base+overlay HDF5 inputs as one layered Web Mercator PNG."""
    paths = list(hdf_paths)
    total_start = time.perf_counter()

    step_start = time.perf_counter()
    base_source, overlay_source = decode_layered_pair(paths, quantity=palette.quantity)
    decode_time = time.perf_counter() - step_start

    step_start = time.perf_counter()
    # One shared target grid so base/overlay stay pixel-aligned after warp/crop.
    target = target_grid([base_source, overlay_source], bounds)
    frame = LayeredFrame(
        base=_warped(base_source, target),
        overlay=_warped(overlay_source, target),
    )
    transform_time = time.perf_counter() - step_start

    emit_timings = EmitTimings()
    result = emit_frame(
        frame,
        output_dir,
        palette,
        base,
        variants,
        optimize,
        sources=[path.name for path in paths],
        timings=emit_timings,
        metadata_bounds=bounds,
        nodata_fill=nodata_fill,
        on_output_ready=on_output_ready,
    )
    log_render_performance(
        base=base,
        source_count=len(paths),
        result=result,
        field=frame,
        total=time.perf_counter() - total_start,
        decode=decode_time,
        transform_label="layered",
        transform=transform_time,
        emit=emit_timings,
    )
    return result


def _warped(source: RadarField, target: GeoTransform) -> RadarField:
    return RadarField(
        values=resample_to_grid(source, target),
        crs=WEB_MERCATOR,
        transform=target,
        quantity=source.quantity,
        timestamp=source.timestamp,
    )
