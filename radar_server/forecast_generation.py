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
        motion_grid_step=forecast.motion_grid_step,
        motion_grid_max=forecast.motion_grid_max,
        fast_idw=forecast.fast_idw,
        fast_warp=forecast.fast_warp,
    )


def generate_forecast_fields(
    fields: Sequence[RadarField],
    *,
    minutes: Sequence[int],
    method: str = "lucaskanade",
    floor_level: float | None = None,
    motion_grid_step: int = 1,
    motion_grid_max: int | None = None,
    fast_idw: bool = False,
    fast_warp: bool = False,
) -> dict[int, RadarField]:
    """Extrapolate ``fields`` (chronological, oldest first) ``minutes`` ahead.

    Values at or below ``floor_level`` become NaN so they render transparent.

    Performance knobs (all optional, off by default):

    * ``motion_grid_step`` > 1 densifies the (smooth) Lucas-Kanade motion field
      on a coarsened grid and upscales it; 1 keeps full resolution.
    * ``motion_grid_max`` caps the motion-interpolation grid to this many pixels
      on its longest edge (overrides ``motion_grid_step`` when it implies a
      coarser grid), making the densification near constant-time across product
      sizes.
    * ``fast_idw`` uses the parallel kd-tree inverse-distance interpolation in
      :mod:`radar_server.forecast_fast` (numerically identical to pysteps).
    * ``fast_warp`` uses the ``cv2.remap`` semi-Lagrangian extrapolation in
      :mod:`radar_server.forecast_fast` instead of scipy.
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
    latest = fields[-1]

    if not np.isfinite(latest.values).any():
        LOGGER.info(
            "Generated transparent forecast fields for %s: latest field contains no finite %s pixels",
            latest.timestamp,
            latest.quantity,
        )
        return _transparent_forecast_fields(latest, unique_minutes)

    # Keep NaN as a radar mask. Motion methods accept masked arrays, and
    # extrapolation allows non-finite values for no-observation pixels.
    motion_input = np.ma.masked_invalid(np.stack([field.values for field in fields]))

    step_start = time.perf_counter()
    velocity = _compute_motion(
        method,
        motion_input,
        grid_step=motion_grid_step,
        grid_max=motion_grid_max,
        fast_idw=fast_idw,
    )
    motion_time = time.perf_counter() - step_start

    lead_steps = [minute / dt for minute in unique_minutes]

    step_start = time.perf_counter()
    forecast_stack = _extrapolate(fields[-1].values, velocity, lead_steps, fast=fast_warp)
    extrapolate_time = time.perf_counter() - step_start

    _log_generation_performance(
        source_count=len(fields),
        lead_count=len(unique_minutes),
        field=latest,
        total=time.perf_counter() - generation_start,
        method=method,
        motion_grid_step=motion_grid_step,
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


def _transparent_forecast_fields(latest: RadarField, minutes: Sequence[int]) -> dict[int, RadarField]:
    generated: dict[int, RadarField] = {}
    for minute in minutes:
        generated[minute] = RadarField(
            values=np.full(latest.values.shape, np.nan, dtype=np.float32),
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
    motion_grid_step: int,
    motion: float,
    extrapolate: float,
) -> None:
    LOGGER.info(
        (
            "Generated forecast fields in %.0fms | sources=%d lead_times=%d "
            "size=%dx%d method=%s motion_step=%d motion=%.0fms extrapolate=%.0fms"
        ),
        total * 1000,
        source_count,
        lead_count,
        field.transform.width,
        field.transform.height,
        method,
        motion_grid_step,
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


# pysteps' Lucas-Kanade default for declustering sparse vectors before the
# dense interpolation; mirrored here so the coarse path tracks identically.
_DECL_SCALE = 20


def _compute_motion(  # noqa: ANN202
    method: str,
    motion_input,  # noqa: ANN001
    *,
    grid_step: int,
    grid_max: int | None = None,
    fast_idw: bool = False,
):
    oflow_method = _motion_method(method)
    if method == "lucaskanade" and (grid_step > 1 or grid_max is not None):
        velocity = _coarse_lucaskanade_velocity(
            oflow_method, motion_input, grid_step=grid_step, grid_max=grid_max, fast_idw=fast_idw
        )
        if velocity is not None:
            return velocity
    kwargs = {"verbose": False} if _accepts_keyword(oflow_method, "verbose") else {}
    return oflow_method(motion_input, **kwargs)


def _coarse_lucaskanade_velocity(  # noqa: ANN202
    oflow_method,  # noqa: ANN001
    motion_input,  # noqa: ANN001
    *,
    grid_step: int,
    grid_max: int | None = None,
    fast_idw: bool = False,
):
    """Lucas-Kanade motion densified on a coarsened grid, then upscaled.

    Tracking and feature detection are cheap; the cost is interpolating the
    sparse vectors onto every pixel. The motion field is smooth, so we run that
    interpolation on a coarsened grid and bilinearly upscale. ``grid_max`` caps
    the longest grid edge so densification cost stays bounded on huge products.

    Returns ``None`` to signal the caller to fall back to the full-grid method
    when the grid is too small to coarsen.
    """
    height, width = motion_input.shape[1:]
    step = max(1, grid_step)
    if grid_max is not None and grid_max > 0:
        step = max(step, int(np.ceil(max(height, width) / grid_max)))
    xgrid = np.arange(0, width, step)
    ygrid = np.arange(0, height, step)
    if xgrid.size < 2 or ygrid.size < 2:
        return None

    kwargs = {"verbose": False} if _accepts_keyword(oflow_method, "verbose") else {}
    xy, uv = oflow_method(motion_input, dense=False, **kwargs)
    if len(xy) == 0:
        return np.zeros((2, height, width), dtype=np.float32)

    idwinterp2d, decluster = _coarse_motion_tools()
    xy, uv = decluster(xy, uv, _DECL_SCALE, 1, False)
    if fast_idw:
        from . import forecast_fast

        coarse = forecast_fast.idw_interpolate(xy, uv, xgrid, ygrid)
    else:
        coarse = idwinterp2d(xy, uv, xgrid, ygrid)
    coarse = np.asarray(coarse, dtype=np.float32)
    return _upscale_velocity(coarse, width, height)


def _extrapolate(precip, velocity, lead_steps, *, fast: bool):  # noqa: ANN001, ANN202
    if fast:
        from . import forecast_fast

        return forecast_fast.extrapolate(
            precip,
            velocity,
            lead_steps,
            allow_nonfinite_values=bool(np.any(~np.isfinite(precip))),
        )
    return _extrapolation_method()(precip, velocity, lead_steps)


def _upscale_velocity(coarse, width: int, height: int):  # noqa: ANN001, ANN202
    import cv2

    u = cv2.resize(np.ascontiguousarray(coarse[0]), (width, height), interpolation=cv2.INTER_LINEAR)
    v = cv2.resize(np.ascontiguousarray(coarse[1]), (width, height), interpolation=cv2.INTER_LINEAR)
    return np.stack([u, v])


def _coarse_motion_tools():  # noqa: ANN202
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        from pysteps.utils import get_method as get_utils_method
        from pysteps.utils.cleansing import decluster

    return get_utils_method("idwinterp2d"), decluster


def _accepts_keyword(func, name: str) -> bool:  # noqa: ANN001
    try:
        parameters = inspect.signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == name for parameter in parameters)
