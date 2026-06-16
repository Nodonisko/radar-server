"""Optimized, portable drop-ins for the two pysteps forecast hotspots.

Profiling forecast generation showed two dominant costs:

* densifying the sparse Lucas-Kanade motion vectors onto the grid
  (``pysteps.utils.interpolate.idwinterp2d``), and
* the semi-Lagrangian warp (``pysteps.extrapolation.semilagrangian`` via
  ``scipy.ndimage.map_coordinates``).

Both have faster, hardware-agnostic equivalents that need no GPU:

* :func:`idw_interpolate` mirrors pysteps' inverse-distance weighting exactly
  but runs the kd-tree query across all cores (``workers=-1``). The result is
  numerically identical to pysteps.
* :func:`extrapolate` reimplements the semi-Lagrangian scheme with
  ``cv2.remap`` (multithreaded + SIMD) instead of ``scipy.ndimage`` for the
  per-pixel warps. Bilinear sampling differs from scipy only at the ~1e-2 level.

These live in radar_server (not a pysteps fork) so stock pysteps stays
upgradable and the optimized surface is small and testable.
"""

from __future__ import annotations

import numpy as np
from scipy.spatial import cKDTree
from scipy.spatial.distance import cdist


def idw_interpolate(
    xy_coord: np.ndarray,
    values: np.ndarray,
    xgrid: np.ndarray,
    ygrid: np.ndarray,
    *,
    power: float = 0.5,
    k: int | None = 20,
    dist_offset: float = 0.5,
) -> np.ndarray:
    """Inverse-distance-weighting interpolation, parallel kd-tree query.

    A faithful reimplementation of ``pysteps.utils.interpolate.idwinterp2d``
    (same ``power``/``k``/``dist_offset`` defaults and the same pixel-distance
    normalisation) with the nearest-neighbour query parallelised over all CPU
    cores. Output matches pysteps to floating-point precision.
    """
    values = np.asarray(values)
    if values.ndim == 1:
        values = values[:, None]
    nvar = values.shape[1]
    npoints = values.shape[0]
    grid_shape = (ygrid.size, xgrid.size)

    # Uniform-output shortcuts (mirror pysteps' interpolator preamble).
    if npoints == 1:
        return (np.ones((nvar,) + grid_shape) * values[0, :][:, None, None]).squeeze()
    if values.max() == values.min():
        return (np.ones((nvar,) + grid_shape) * values.ravel()[0]).squeeze()

    xgridv, ygridv = np.meshgrid(xgrid, ygrid)
    gridv = np.column_stack((xgridv.ravel(), ygridv.ravel()))

    if k is not None:
        k = int(np.min((k, npoints)))
        tree = cKDTree(xy_coord)
        dist, inds = tree.query(gridv, k=k, workers=-1)
        if dist.ndim == 1:
            dist = dist[..., None]
            inds = inds[..., None]
    else:
        dist = cdist(xy_coord, gridv, "euclidean").transpose()
        inds = np.arange(npoints)[None, :] * np.ones((gridv.shape[0], npoints)).astype(int)

    x_res = np.gradient(xgrid)
    y_res = np.gradient(ygrid)
    mean_res = np.mean(np.abs([x_res.mean(), y_res.mean()]))
    dist = dist / mean_res

    dist = dist + dist_offset
    weights = 1.0 / np.power(dist, power)
    weights = weights / np.sum(weights, axis=1, keepdims=True)

    output_array = np.sum(values[inds, :] * weights[..., None], axis=1)
    output_array = output_array.reshape(ygrid.size, xgrid.size, nvar)
    return np.moveaxis(output_array, -1, 0).squeeze()


def extrapolate(
    precip: np.ndarray,
    velocity: np.ndarray,
    timesteps,
    *,
    outval: float = np.nan,
    allow_nonfinite_values: bool = False,
    vel_timestep: float = 1.0,
    n_iter: int = 1,
    interp_order: int = 1,
) -> np.ndarray:
    """Semi-Lagrangian backward extrapolation using ``cv2.remap`` warps.

    Mirrors ``pysteps.extrapolation.semilagrangian.extrapolate`` (midpoint-rule
    trajectory integration with ``n_iter`` inner iterations) but performs every
    per-pixel warp with OpenCV's multithreaded ``remap`` instead of
    ``scipy.ndimage.map_coordinates``. Only the linear interpolation path
    (``interp_order == 1``) is supported; callers needing cubic should use the
    pysteps implementation.
    """
    import cv2

    if interp_order != 1:
        raise ValueError(f"fast extrapolate supports interp_order=1 only, got {interp_order}")
    if not allow_nonfinite_values and precip is not None and np.any(~np.isfinite(precip)):
        raise ValueError("precip contains non-finite values")

    if isinstance(timesteps, int):
        timesteps = np.arange(1, timesteps + 1)
        vel_timestep = 1.0
    timesteps = list(timesteps)
    if sorted(timesteps) != timesteps:
        raise ValueError("timesteps is not in ascending order")
    timestep_diff = np.hstack([[timesteps[0]], np.diff(timesteps)])

    height, width = int(velocity.shape[1]), int(velocity.shape[2])
    x_values, y_values = np.meshgrid(np.arange(width), np.arange(height))
    xy_coords = np.stack([x_values, y_values]).astype(np.float32)

    vel = np.asarray(velocity, dtype=np.float32)
    precip32 = None if precip is None else np.asarray(precip, dtype=np.float32)

    def warp(field: np.ndarray, coords: np.ndarray, border_mode: int, border_value: float) -> np.ndarray:
        map_x = np.ascontiguousarray(coords[0], dtype=np.float32)
        map_y = np.ascontiguousarray(coords[1], dtype=np.float32)
        return cv2.remap(
            field,
            map_x,
            map_y,
            interpolation=cv2.INTER_LINEAR,
            borderMode=border_mode,
            borderValue=border_value,
        )

    def interpolate_motion(displacement: np.ndarray, velocity_inc: np.ndarray, td: float) -> None:
        coords = xy_coords + displacement
        velocity_inc[0] = warp(vel[0], coords, cv2.BORDER_REPLICATE, 0.0)
        velocity_inc[1] = warp(vel[1], coords, cv2.BORDER_REPLICATE, 0.0)
        if n_iter > 1:
            velocity_inc /= n_iter
        velocity_inc *= td / vel_timestep

    precip_extrap: list[np.ndarray] = []
    displacement = np.zeros((2, height, width), dtype=np.float32)
    velocity_inc = vel.copy() * timestep_diff[0] / vel_timestep

    for ti, td in enumerate(timestep_diff):
        if n_iter > 0:
            for _ in range(n_iter):
                interpolate_motion(displacement - velocity_inc / 2.0, velocity_inc, td)
                displacement -= velocity_inc
                interpolate_motion(displacement, velocity_inc, td)
        else:
            if ti > 0:
                interpolate_motion(displacement, velocity_inc, td)
            displacement -= velocity_inc

        if precip32 is not None:
            coords = xy_coords + displacement
            warped = warp(precip32, coords, cv2.BORDER_CONSTANT, float(outval))
            precip_extrap.append(warped)

    return np.stack(precip_extrap)
