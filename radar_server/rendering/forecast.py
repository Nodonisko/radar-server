"""Forecast rendering using pysteps."""

from __future__ import annotations

import contextlib
import inspect
import io
import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import Sequence

import numpy as np

from .core import PaletteSpec, RadarField
from .pipeline import _EmitTimings, _emit, RenderResult

LOGGER = logging.getLogger(__name__)


def render_forecast(
    fields: Sequence[RadarField],
    output_dir: Path,
    palette: PaletteSpec,
    *,
    base: str,
    forecast_minutes: Sequence[int],
    method: str = "VET",
    variants: Sequence[tuple[str, float]] = (("overlay", 1.0), ("overlay_small", 1.5)),
    optimize: bool = True,
) -> list[RenderResult]:
    """Generate forecast frames using pysteps and render them to PNGs.

    `fields` should be a sequence of past RadarFields, ordered chronologically
    (oldest to newest).
    """
    if len(fields) < 2:
        LOGGER.warning("Not enough fields for forecast. Need at least 2, got %d", len(fields))
        return []

    minutes = tuple(dict.fromkeys(sorted(forecast_minutes)))
    if not minutes:
        LOGGER.info("Forecast skipped for %s: no forecast_minutes requested", base)
        return []
    if any(minute <= 0 for minute in minutes):
        raise ValueError(f"forecast minutes must be positive, got {minutes!r}")

    dt = (fields[-1].timestamp - fields[-2].timestamp).total_seconds() / 60.0
    if dt <= 0:
        raise ValueError("fields must be ordered chronologically with increasing timestamps")

    forecast_start = time.perf_counter()

    # Keep NaN as a radar mask. VET accepts masked arrays, and extrapolation
    # allows non-finite values for no-observation pixels.
    motion_input = np.ma.masked_invalid(np.stack([field.values for field in fields]))

    step_start = time.perf_counter()
    oflow_method = motion.get_method(method)
    velocity = _compute_motion(oflow_method, motion_input)
    motion_time = time.perf_counter() - step_start

    extrapolate = nowcasts.get_method("extrapolation")
    lead_steps = [minute / dt for minute in minutes]

    step_start = time.perf_counter()
    forecast_stack = extrapolate(fields[-1].values, velocity, lead_steps)
    extrapolate_time = time.perf_counter() - step_start
    _log_forecast_compute_performance(
        base=base,
        source_count=len(fields),
        lead_count=len(minutes),
        field=fields[-1],
        total=time.perf_counter() - forecast_start,
        method=method,
        motion=motion_time,
        extrapolate=extrapolate_time,
    )

    results = []

    for idx, minute in enumerate(minutes):
        forecast_data = np.asarray(forecast_stack[idx], dtype=np.float32).copy()
        forecast_data[forecast_data <= palette.levels[0]] = np.nan

        forecast_field = RadarField(
            values=forecast_data,
            crs=fields[-1].crs,
            transform=fields[-1].transform,
            quantity=fields[-1].quantity,
            timestamp=fields[-1].timestamp + timedelta(minutes=minute),
        )

        forecast_base = f"{base}_fct{minute}"
        emit_timings = _EmitTimings()

        result = _emit(
            forecast_field,
            output_dir,
            palette,
            forecast_base,
            variants,
            optimize,
            sources=[f"forecast_{minute}m"],
            timings=emit_timings,
        )

        _log_forecast_emit_performance(base=forecast_base, result=result, field=forecast_field, emit=emit_timings)
        results.append(result)

    return results


def _log_forecast_compute_performance(
    *,
    base: str,
    source_count: int,
    lead_count: int,
    field: RadarField,
    total: float,
    method: str,
    motion: float,
    extrapolate: float,
) -> None:
    LOGGER.info(
        (
            "Computed forecast %s in %.0fms | sources=%d lead_times=%d "
            "size=%dx%d method=%s motion=%.0fms extrapolate=%.0fms"
        ),
        base,
        total * 1000,
        source_count,
        lead_count,
        field.transform.width,
        field.transform.height,
        method,
        motion * 1000,
        extrapolate * 1000,
    )


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


def _import_pysteps_modules():
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        from pysteps import motion, nowcasts

    return motion, nowcasts


def _compute_motion(oflow_method, motion_input):  # noqa: ANN001
    kwargs = {"verbose": False} if _accepts_keyword(oflow_method, "verbose") else {}
    return oflow_method(motion_input, **kwargs)


def _accepts_keyword(func, name: str) -> bool:  # noqa: ANN001
    try:
        parameters = inspect.signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == name for parameter in parameters)


motion, nowcasts = _import_pysteps_modules()
