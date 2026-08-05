"""Forecast field generation: pysteps motion estimation plus extrapolation.

This is the compute half of nowcasting, split from rendering. It produces
in-memory :class:`RadarField` objects per lead minute (or, for layered
products, physical base+overlay :class:`LayeredFrame` pairs); persisting them
is the job of ``forecast_store`` and rendering them the job of the render lane.

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

from .config import ForecastProduct
from .queueing import ForecastGenTask
from .render_jobs import bounds_tuple
from .rendering.composite import composite_to_web_mercator, target_grid
from .rendering.core import WEB_MERCATOR, GeoTransform, RadarField
from .rendering.decode import load_odim_hdf
from .rendering.layered import LayeredFrame, decode_layered_pair
from .rendering.reproject import resample_to_grid, to_web_mercator

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


def load_layered_history(task: ForecastGenTask) -> tuple[list[RadarField], RadarField]:
    """Overlay history plus the latest base field, on one shared grid.

    Each ``HistoryFrame.paths`` must be ``(base, overlay)`` in product input
    order — for Czechia, CAPPI then MAX-Z. Motion needs the full overlay
    history, but only the newest base is warped into the forecast, so older
    base files are never decoded.
    """

    forecast = task.forecast
    bounds = bounds_tuple(forecast.geo_bounds)
    quantity = forecast.palette.quantity
    frames = task.history
    if not frames:
        raise ValueError(f"layered forecast {forecast.id!r} has no history frames")
    for frame in frames:
        if len(frame.paths) != 2:
            raise ValueError(
                f"layered forecast {forecast.id!r} needs 2 input paths per history "
                f"frame, got {len(frame.paths)} at {frame.timestamp}"
            )

    base_source, overlay_latest_source = decode_layered_pair(frames[-1].paths, quantity=quantity)
    # One shared target grid keeps history frames stackable for motion and the
    # latest pair pixel-aligned for the layered merge-at-colorize.
    target = target_grid([base_source, overlay_latest_source], bounds)

    overlay_history = [
        _warped(load_odim_hdf(frame.paths[1], quantity=quantity), target) for frame in frames[:-1]
    ]
    overlay_history.append(_warped(overlay_latest_source, target))
    return overlay_history, _warped(base_source, target)


def _warped(source: RadarField, target: GeoTransform) -> RadarField:
    return RadarField(
        values=resample_to_grid(source, target),
        crs=WEB_MERCATOR,
        transform=target,
        quantity=source.quantity,
        timestamp=source.timestamp,
    )


def generate_for_task(task: ForecastGenTask) -> dict[int, RadarField | LayeredFrame]:
    """Load history from disk and generate all lead fields for one gen task."""

    forecast = task.forecast
    if forecast.layered:
        overlay_history, base_latest = load_layered_history(task)
        return generate_layered_forecast_fields(
            overlay_history,
            base_latest,
            minutes=forecast.minutes,
            **generation_options(forecast),
        )

    fields = load_history_fields(task)
    return generate_forecast_fields(
        fields,
        minutes=forecast.minutes,
        floor_level=forecast.palette.levels[0],
        **generation_options(forecast),
    )


def generation_options(forecast: ForecastProduct) -> dict:
    """Per-forecast tuning knobs shared by the plain and layered generators."""

    return dict(
        method=forecast.method,
        motion_grid_step=forecast.motion_grid_step,
        motion_grid_max=forecast.motion_grid_max,
        fast_idw=forecast.fast_idw,
        fast_warp=forecast.fast_warp,
        fast_motion=forecast.fast_motion,
        warp_grid_step=forecast.warp_grid_step,
        forecast_id=forecast.id,
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
    fast_motion: bool = False,
    warp_grid_step: int = 1,
    forecast_id: str | None = None,
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
    * ``fast_motion`` uses the MaskedArray-free sparse Lucas-Kanade path in
      :mod:`radar_server.forecast_fast` (numerically identical to pysteps).
    * ``fast_idw`` uses the parallel kd-tree + numba inverse-distance
      interpolation in :mod:`radar_server.forecast_fast` (numerically identical
      to pysteps).
    * ``fast_warp`` uses the ``cv2.remap`` semi-Lagrangian extrapolation in
      :mod:`radar_server.forecast_fast` instead of scipy. ``warp_grid_step`` > 1
      integrates that warp's displacement trajectory on a coarsened grid (only
      used when ``fast_warp`` is on).
    """

    if len(fields) < 2:
        raise ValueError(f"forecast needs at least 2 history fields, got {len(fields)}")

    unique_minutes = _validated_minutes(minutes)
    if not unique_minutes:
        return {}

    latest = fields[-1]
    if not np.isfinite(latest.values).any():
        LOGGER.info(
            "Generated transparent forecast fields for %s: latest field contains no finite %s pixels",
            latest.timestamp,
            latest.quantity,
        )
        return _transparent_forecast_fields(latest, unique_minutes)

    [forecast_stack] = _extrapolate_history(
        fields,
        [latest.values],
        unique_minutes=unique_minutes,
        method=method,
        motion_grid_step=motion_grid_step,
        motion_grid_max=motion_grid_max,
        fast_idw=fast_idw,
        fast_warp=fast_warp,
        fast_motion=fast_motion,
        warp_grid_step=warp_grid_step,
        forecast_id=forecast_id,
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


def generate_layered_forecast_fields(
    overlay_history: Sequence[RadarField],
    base_latest: RadarField,
    *,
    minutes: Sequence[int],
    method: str = "lucaskanade",
    motion_grid_step: int = 1,
    motion_grid_max: int | None = None,
    fast_idw: bool = False,
    fast_warp: bool = False,
    fast_motion: bool = False,
    warp_grid_step: int = 1,
    forecast_id: str | None = None,
) -> dict[int, RadarField | LayeredFrame]:
    """Extrapolate base+overlay with shared motion into layered lead pairs.

    Motion is estimated from the denser ``overlay_history`` (MAX-Z); its tip
    and ``base_latest`` are then warped with the same velocity. Each lead stays
    a physical :class:`LayeredFrame` — the faded-overlay presentation happens
    at colorize time, so no sentinel values are generated or stored.
    """

    if len(overlay_history) < 2:
        raise ValueError(f"forecast needs at least 2 history fields, got {len(overlay_history)}")
    overlay_latest = overlay_history[-1]
    if base_latest.timestamp != overlay_latest.timestamp:
        raise ValueError(
            f"layered latest fields must share a timestamp, got "
            f"{base_latest.timestamp} vs {overlay_latest.timestamp}"
        )

    unique_minutes = _validated_minutes(minutes)
    if not unique_minutes:
        return {}

    if not np.isfinite(base_latest.values).any() and not np.isfinite(overlay_latest.values).any():
        LOGGER.info(
            "Generated transparent layered forecast fields for %s: latest fields contain no finite %s pixels",
            overlay_latest.timestamp,
            overlay_latest.quantity,
        )
        return _transparent_forecast_fields(overlay_latest, unique_minutes)

    base_stack, overlay_stack = _extrapolate_history(
        overlay_history,
        [base_latest.values, overlay_latest.values],
        unique_minutes=unique_minutes,
        method=method,
        motion_grid_step=motion_grid_step,
        motion_grid_max=motion_grid_max,
        fast_idw=fast_idw,
        fast_warp=fast_warp,
        fast_motion=fast_motion,
        warp_grid_step=warp_grid_step,
        forecast_id=forecast_id,
    )

    generated: dict[int, RadarField | LayeredFrame] = {}
    for idx, minute in enumerate(unique_minutes):
        lead_timestamp = overlay_latest.timestamp + timedelta(minutes=minute)
        generated[minute] = LayeredFrame(
            base=RadarField(
                values=np.asarray(base_stack[idx], dtype=np.float32),
                crs=base_latest.crs,
                transform=base_latest.transform,
                quantity=base_latest.quantity,
                timestamp=lead_timestamp,
            ),
            overlay=RadarField(
                values=np.asarray(overlay_stack[idx], dtype=np.float32),
                crs=overlay_latest.crs,
                transform=overlay_latest.transform,
                quantity=overlay_latest.quantity,
                timestamp=lead_timestamp,
            ),
        )
    return generated


def _validated_minutes(minutes: Sequence[int]) -> tuple[int, ...]:
    unique_minutes = tuple(dict.fromkeys(sorted(minutes)))
    if any(minute <= 0 for minute in unique_minutes):
        raise ValueError(f"forecast minutes must be positive, got {unique_minutes!r}")
    return unique_minutes


def _extrapolate_history(
    motion_fields: Sequence[RadarField],
    warp_values: Sequence[np.ndarray],
    *,
    unique_minutes: tuple[int, ...],
    method: str,
    motion_grid_step: int,
    motion_grid_max: int | None,
    fast_idw: bool,
    fast_warp: bool,
    fast_motion: bool,
    warp_grid_step: int,
    forecast_id: str | None,
) -> list[np.ndarray]:
    """Shared core: motion from ``motion_fields``, then warp each field.

    Returns one ``(leads, H, W)`` stack per entry of ``warp_values``, all
    driven by a single velocity field (and, on the fast path, a single
    displacement trajectory).
    """

    dt = (motion_fields[-1].timestamp - motion_fields[-2].timestamp).total_seconds() / 60.0
    if dt <= 0:
        raise ValueError("fields must be ordered chronologically with increasing timestamps")

    generation_start = time.perf_counter()
    motion_stack = np.stack([field.values for field in motion_fields])

    step_start = time.perf_counter()
    velocity = _compute_motion(
        method,
        motion_stack,
        grid_step=motion_grid_step,
        grid_max=motion_grid_max,
        fast_idw=fast_idw,
        fast_motion=fast_motion,
    )
    motion_time = time.perf_counter() - step_start

    lead_steps = [minute / dt for minute in unique_minutes]

    step_start = time.perf_counter()
    stacks = _extrapolate_many(warp_values, velocity, lead_steps, fast=fast_warp, grid_step=warp_grid_step)
    extrapolate_time = time.perf_counter() - step_start

    _log_generation_performance(
        forecast_id=forecast_id,
        source_count=len(motion_fields),
        lead_count=len(unique_minutes),
        field=motion_fields[-1],
        total=time.perf_counter() - generation_start,
        method=method,
        motion_grid_step=motion_grid_step,
        motion=motion_time,
        extrapolate=extrapolate_time,
    )
    return stacks


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
    forecast_id: str | None,
    source_count: int,
    lead_count: int,
    field: RadarField,
    total: float,
    method: str,
    motion_grid_step: int,
    motion: float,
    extrapolate: float,
) -> None:
    if forecast_id is None:
        subject = "Generated forecast fields"
    else:
        subject = f"Generated {forecast_id} forecast fields"
    LOGGER.info(
        (
            "%s in %.0fms | sources=%d lead_times=%d "
            "size=%dx%d method=%s motion_step=%d motion=%.0fms extrapolate=%.0fms"
        ),
        subject,
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
    motion_stack,  # noqa: ANN001
    *,
    grid_step: int,
    grid_max: int | None = None,
    fast_idw: bool = False,
    fast_motion: bool = False,
):
    oflow_method = _motion_method(method)
    if method == "lucaskanade" and (fast_motion or grid_step > 1 or grid_max is not None):
        velocity = _coarse_lucaskanade_velocity(
            oflow_method,
            motion_stack,
            grid_step=grid_step,
            grid_max=grid_max,
            fast_idw=fast_idw,
            fast_motion=fast_motion,
        )
        if velocity is not None:
            return velocity
    # Full-resolution pysteps path consumes a MaskedArray (NaN -> no-data mask).
    motion_input = np.ma.masked_invalid(motion_stack)
    kwargs = {"verbose": False} if _accepts_keyword(oflow_method, "verbose") else {}
    return oflow_method(motion_input, **kwargs)


def _coarse_lucaskanade_velocity(  # noqa: ANN202
    oflow_method,  # noqa: ANN001
    motion_stack,  # noqa: ANN001
    *,
    grid_step: int,
    grid_max: int | None = None,
    fast_idw: bool = False,
    fast_motion: bool = False,
):
    """Lucas-Kanade motion densified on a (optionally coarsened) grid, upscaled.

    Tracking and feature detection are cheap; the cost is interpolating the
    sparse vectors onto every pixel. The motion field is smooth, so we run that
    interpolation on a coarsened grid and bilinearly upscale. ``grid_max`` caps
    the longest grid edge so densification cost stays bounded on huge products.

    ``fast_motion`` swaps pysteps' MaskedArray-based sparse extraction for the
    plain-array path in :mod:`radar_server.forecast_fast` (same vectors, far
    less ``numpy.ma`` overhead).

    Returns ``None`` to signal the caller to fall back to the full-grid method
    when the grid is too small to coarsen.
    """
    height, width = motion_stack.shape[1:]
    step = max(1, grid_step)
    if grid_max is not None and grid_max > 0:
        step = max(step, int(np.ceil(max(height, width) / grid_max)))
    xgrid = np.arange(0, width, step)
    ygrid = np.arange(0, height, step)
    if xgrid.size < 2 or ygrid.size < 2:
        return None

    if fast_motion:
        from . import forecast_fast

        xy, uv = forecast_fast.lk_sparse_vectors(motion_stack)
    else:
        motion_input = np.ma.masked_invalid(motion_stack)
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


def _extrapolate_many(
    values_list: Sequence[np.ndarray],
    velocity,  # noqa: ANN001
    lead_steps: Sequence[float],
    *,
    fast: bool,
    grid_step: int = 1,
) -> list[np.ndarray]:
    """Extrapolate several fields with one velocity; one stack per field.

    The fast path integrates the displacement trajectory once and warps every
    field with it, so extra fields cost one ``cv2.remap`` per lead each. The
    pysteps fallback integrates internally and simply runs once per field.
    """
    if fast:
        from . import forecast_fast

        return forecast_fast.extrapolate_many(
            values_list,
            velocity,
            lead_steps,
            allow_nonfinite_values=any(bool(np.any(~np.isfinite(v))) for v in values_list),
            grid_step=grid_step,
        )
    extrapolate = _extrapolation_method()
    return [extrapolate(values, velocity, lead_steps) for values in values_list]


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
