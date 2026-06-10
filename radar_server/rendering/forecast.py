"""Render precomputed forecast fields to PNG variants.

The compute half (pysteps motion + extrapolation) lives in
``radar_server.forecast_generation``; this module only emits a ready
:class:`RadarField` through the shared downsample/colorize/encode tail, so a
forecast frame renders exactly like an observed frame minus decode/reproject.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

from .core import PaletteSpec, RadarField
from .pipeline import DEFAULT_VARIANTS, RenderResult, _EmitTimings, _emit

LOGGER = logging.getLogger(__name__)


def render_forecast_field(
    field: RadarField,
    output_dir: Path,
    palette: PaletteSpec,
    *,
    base: str,
    minute: int | None = None,
    variants: Sequence[tuple[str, float]] = DEFAULT_VARIANTS,
    optimize: bool = True,
) -> RenderResult:
    """Render one forecast lead frame to PNG variants plus its JSON sidecar."""

    emit_timings = _EmitTimings()
    source = f"forecast_{minute}m" if minute is not None else f"forecast_{base}"
    result = _emit(
        field,
        output_dir,
        palette,
        base,
        variants,
        optimize,
        sources=[source],
        timings=emit_timings,
    )
    _log_forecast_emit_performance(base=base, result=result, field=field, emit=emit_timings)
    return result


def _log_forecast_emit_performance(
    *,
    base: str,
    result: RenderResult,
    field: RadarField,
    emit: _EmitTimings,
) -> None:
    LOGGER.info(
        (
            "Rendered %s in %.0fms | forecast_frame variants=%d size=%dx%d "
            "emit=%.0fms (downsample=%.0fms colorize=%.0fms png_write=%.0fms "
            "(png_save=%.0fms oxipng=%.0fms))"
        ),
        base,
        emit.total * 1000,
        len(result.variants),
        field.transform.width,
        field.transform.height,
        emit.total * 1000,
        emit.downsample * 1000,
        emit.colorize * 1000,
        emit.png_write * 1000,
        emit.png_save * 1000,
        emit.oxipng * 1000,
    )
