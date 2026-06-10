"""Forecast field generation: pysteps motion estimation plus extrapolation.

This is the compute half of nowcasting, split from rendering. It produces
in-memory :class:`RadarField` objects per lead minute; persisting them is the
job of ``forecast_store`` and rendering them the job of the render lane.

pysteps is imported lazily so that importing this module stays cheap.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import logging
import time
from datetime import timedelta
from typing import Sequence

import numpy as np

from .queueing import ForecastGenTask
from .render_jobs import bounds_tuple
from .rendering.composite import composite_to_web_mercator
from .rendering.core import RadarField
from .rendering.decode import load_odim_hdf
from .rendering.reproject import to_web_mercator

LOGGER = logging.getLogger(__name__)


def load_history_fields(task: ForecastGenTask) -> list[RadarField]:
    """Decode and reproject the parent's history frames for motion estimation."""

    forecast = task.forecast
    bounds = bounds_tuple(forecast.geo_bounds)
    quantity = forecast.palette.quantity
    fields: list[RadarField] = []
    for frame in task.history:
        if len(frame.paths) == 1 and bounds is None:
            field = to_web_mercator(load_odim_hdf(frame.paths[0], quantity=quantity))
        else:
            source_fields = [load_odim_hdf(path, quantity=quantity) for path in frame.paths]
            field = composite_to_web_mercator(source_fields, bounds=bounds)
        fields.append(field)
    return fields


def generate_for_task(task: ForecastGenTask) -> dict[int, RadarField]:
    """Load history from disk and generate all lead fields for one gen task."""

    forecast = task.forecast
    fields = load_history_fields(task)
    return generate_forecast_fields(
        fields,
        minutes=forecast.minutes,
        method=forecast.method,
        floor_level=forecast.palette.levels[0],
    )


def generate_forecast_fields(
    fields: Sequence[RadarField],
    *,
    minutes: Sequence[int],
    method: str = "lucaskanade",
    floor_level: float | None = None,
) -> dict[int, RadarField]:
    """Extrapolate ``fields`` (chronological, oldest first) ``minutes`` ahead.

    Values at or below ``floor_level`` become NaN so they render transparent.
    """

    if len(fields) < 2:
        raise ValueError(f"forecast needs at least 2 history fields, got {len(fields)}")

    unique_minutes = tuple(dict.fromkeys(sorted(minutes)))
    if not unique_minutes:
        return {}
    if any(minute <= 0 for minute in unique_minutes):
        raise ValueError(f"forecast minutes must be positive, got {unique_minutes!r}")

    dt = (fields[-1].timestamp - fields[-2].timestamp).total_seconds() / 60.0
    if dt <= 0:
        raise ValueError("fields must be ordered chronologically with increasing timestamps")

    generation_start = time.perf_counter()

    # Keep NaN as a radar mask. Motion methods accept masked arrays, and
    # extrapolation allows non-finite values for no-observation pixels.
    motion_input = np.ma.masked_invalid(np.stack([field.values for field in fields]))

    step_start = time.perf_counter()
    velocity = _compute_motion(_motion_method(method), motion_input)
    motion_time = time.perf_counter() - step_start

    lead_steps = [minute / dt for minute in unique_minutes]

    step_start = time.perf_counter()
    forecast_stack = _extrapolation_method()(fields[-1].values, velocity, lead_steps)
    extrapolate_time = time.perf_counter() - step_start

    latest = fields[-1]
    _log_generation_performance(
        source_count=len(fields),
        lead_count=len(unique_minutes),
        field=latest,
        total=time.perf_counter() - generation_start,
        method=method,
        motion=motion_time,
        extrapolate=extrapolate_time,
    )

    generated: dict[int, RadarField] = {}
    for idx, minute in enumerate(unique_minutes):
        data = np.asarray(forecast_stack[idx], dtype=np.float32).copy()
        if floor_level is not None:
            data[data <= floor_level] = np.nan
        generated[minute] = RadarField(
            values=data,
            crs=latest.crs,
            transform=latest.transform,
            quantity=latest.quantity,
            timestamp=latest.timestamp + timedelta(minutes=minute),
        )
    return generated


def _log_generation_performance(
    *,
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
            "Generated forecast fields in %.0fms | sources=%d lead_times=%d "
            "size=%dx%d method=%s motion=%.0fms extrapolate=%.0fms"
        ),
        total * 1000,
        source_count,
        lead_count,
        field.transform.width,
        field.transform.height,
        method,
        motion * 1000,
        extrapolate * 1000,
    )


def _motion_method(method: str):  # noqa: ANN202
    motion, _ = _import_pysteps_modules()
    return motion.get_method(method)


def _extrapolation_method():  # noqa: ANN202
    _, nowcasts = _import_pysteps_modules()
    return nowcasts.get_method("extrapolation")


def _import_pysteps_modules():  # noqa: ANN202
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        from pysteps import motion, nowcasts

    return motion, nowcasts


def _compute_motion(oflow_method, motion_input):  # noqa: ANN001, ANN202
    kwargs = {"verbose": False} if _accepts_keyword(oflow_method, "verbose") else {}
    return oflow_method(motion_input, **kwargs)


def _accepts_keyword(func, name: str) -> bool:  # noqa: ANN001
    try:
        parameters = inspect.signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == name for parameter in parameters)
