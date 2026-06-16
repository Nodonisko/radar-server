"""Optimized, portable drop-ins for the pysteps forecast hotspots.

Profiling forecast generation showed three dominant costs:

* the sparse Lucas-Kanade feature detection + tracking
  (``pysteps.motion.lucaskanade.dense_lucaskanade``), where most of the time is
  ``numpy.ma`` mask bookkeeping rather than the OpenCV work, because radar
  fields are mostly no-data,
* densifying the sparse motion vectors onto the grid
  (``pysteps.utils.interpolate.idwinterp2d``), and
* the semi-Lagrangian warp (``pysteps.extrapolation.semilagrangian`` via
  ``scipy.ndimage.map_coordinates``).

All three have faster, hardware-agnostic equivalents that need no GPU:

* :func:`lk_sparse_vectors` mirrors ``dense_lucaskanade(dense=False)`` (morph
  opening -> Shi-Tomasi detection -> ``calcOpticalFlowPyrLK`` -> outlier
  removal) but runs on plain ``float32`` arrays with the no-data mask computed
  once, instead of round-tripping a ``MaskedArray`` through every sub-call.
* :func:`idw_interpolate` mirrors pysteps' inverse-distance weighting exactly
  but runs the kd-tree query across all cores (``workers=-1``) and fuses the
  weighting + weighted sum in a single numba parallel kernel. The result is
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

try:  # numba is a hard dep of the fast path, but keep the module importable without it
    from numba import njit, prange

    _HAVE_NUMBA = True
except Exception:  # pragma: no cover - exercised only on installs without numba
    _HAVE_NUMBA = False


if _HAVE_NUMBA:

    @njit(parallel=True, cache=True)
    def _idw_accumulate(dist, inds, values, power, inv_mean_res, dist_offset, out):
        """Fused IDW gather + weighting + normalised sum, parallel over grid points.

        Equivalent to ``sum_j w_j * values[inds_j] / sum_j w_j`` with
        ``w_j = (dist_j / mean_res + dist_offset) ** -power`` — the same weights
        pysteps applies, but without materialising the ``(grid, k, nvar)``
        gather/product temporaries (the dominant cost in the numpy version).
        """
        n_grid = dist.shape[0]
        k = dist.shape[1]
        nvar = values.shape[1]
        for g in prange(n_grid):
            denom = 0.0
            for v in range(nvar):
                out[g, v] = 0.0
            for j in range(k):
                d = dist[g, j] * inv_mean_res + dist_offset
                w = d ** (-power)
                denom += w
                idx = inds[g, j]
                for v in range(nvar):
                    out[g, v] += w * values[idx, v]
            inv = 1.0 / denom
            for v in range(nvar):
                out[g, v] *= inv


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
    """Inverse-distance-weighting interpolation, parallel kd-tree + fused kernel.

    A faithful reimplementation of ``pysteps.utils.interpolate.idwinterp2d``
    (same ``power``/``k``/``dist_offset`` defaults and the same pixel-distance
    normalisation). The nearest-neighbour query runs across all CPU cores
    (``workers=-1``) and, when numba is available, the weighting + weighted sum
    run in a single fused parallel kernel that avoids the large gather/product
    temporaries. Output matches pysteps to floating-point precision.
    """
    values = np.asarray(values, dtype=np.float64)
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

    x_res = np.gradient(xgrid)
    y_res = np.gradient(ygrid)
    mean_res = float(np.mean(np.abs([x_res.mean(), y_res.mean()])))

    if k is not None and _HAVE_NUMBA:
        k = int(np.min((k, npoints)))
        tree = cKDTree(xy_coord)
        dist, inds = tree.query(gridv, k=k, workers=-1)
        if dist.ndim == 1:
            dist = dist[:, None]
            inds = inds[:, None]
        out = np.empty((gridv.shape[0], nvar), dtype=np.float64)
        _idw_accumulate(
            np.ascontiguousarray(dist, dtype=np.float64),
            np.ascontiguousarray(inds, dtype=np.int64),
            np.ascontiguousarray(values, dtype=np.float64),
            float(power),
            1.0 / mean_res,
            float(dist_offset),
            out,
        )
        output_array = out.reshape(ygrid.size, xgrid.size, nvar)
        return np.moveaxis(output_array, -1, 0).squeeze()

    # Portable numpy fallback (numba missing, or the exhaustive k=None path).
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


def _prepare_frame(frame: np.ndarray, size_opening: int):
    """Replicate pysteps' per-frame preprocessing on a plain array.

    Returns ``(filled, valid, vmin)`` where ``filled`` is the no-data-filled
    ``float32`` image (small isolated echoes removed by a morphological opening,
    exactly as ``pysteps.utils.images.morph_opening`` does), ``valid`` is the
    finite-pixel mask, and ``vmin`` is the minimum over valid pixels (pysteps'
    fill value). Returns ``None`` when the frame has no finite pixels.
    """
    import cv2

    valid = np.isfinite(frame)
    if not valid.any():
        return None
    vmin = np.float32(frame[valid].min())
    filled = np.where(valid, frame, vmin).astype(np.float32, copy=False)

    if size_opening > 0:
        field_bin = (filled > vmin).astype(np.uint8)
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (size_opening, size_opening))
        field_bin_out = cv2.morphologyEx(field_bin, cv2.MORPH_OPEN, kernel)
        isolated = (field_bin - field_bin_out) > 0
        if isolated.any():
            filled = filled.copy()
            filled[isolated] = vmin

    return filled, valid, vmin


def _scale_to_uint8(filled: np.ndarray, im_min: np.float32, im_max: np.float32) -> np.ndarray:
    """pysteps' 0-255 rescale + 8-bit cast (same float32 arithmetic / wrap)."""
    if im_max - im_min > 1e-8:
        scaled = (filled - im_min) / (im_max - im_min) * 255
    else:
        scaled = filled - im_min
    return np.ndarray.astype(scaled, "uint8")


def _frame_uint8(filled: np.ndarray, valid: np.ndarray, vmin: np.float32) -> np.ndarray:
    """0-255 uint8 image used for both detection and tracking.

    Mirrors ``pysteps`` (track_features and shitomasi.detection): no-data filled
    with the valid-pixel minimum, rescaled over the full valid range. pysteps'
    detection appears to also buffer the mask before scaling, but it applies the
    buffer with a ``uint8`` mask (integer/row indexing, not boolean), so the
    buffer only shapes the OpenCV search mask -- the detection image is
    byte-identical to the tracking image.
    """
    im_min = vmin
    im_max = np.float32(filled[valid].max())
    image = np.where(valid, filled, vmin).astype(np.float32, copy=False)
    return _scale_to_uint8(image, im_min, im_max)


def _detect_points(
    image_u8: np.ndarray,
    valid: np.ndarray,
    *,
    buffer_mask: int,
    detect_kwargs: dict,
) -> np.ndarray:
    """Shi-Tomasi detection (``pysteps.feature.shitomasi.detection``).

    Searches ``image_u8`` for corners, restricted to the no-data mask buffered
    by ``buffer_mask`` pixels (so features near data edges are dropped).
    """
    import cv2

    mask = (~valid).astype(np.uint8)
    if buffer_mask > 0:
        mask = cv2.dilate(mask, np.ones((int(buffer_mask), int(buffer_mask)), np.uint8), 1)
    cv2_mask = (~mask & 1).astype(np.uint8)

    points = cv2.goodFeaturesToTrack(image_u8, mask=cv2_mask, **detect_kwargs)
    if points is None:
        return np.empty((0, 2), dtype=np.float32)
    return points[:, 0, :].astype(np.float32)


def lk_sparse_vectors(
    images: np.ndarray,
    *,
    size_opening: int = 3,
    buffer_mask: int = 5,
    max_corners: int = 1000,
    quality_level: float = 0.01,
    min_distance: int = 10,
    block_size: int = 5,
    use_harris: bool = False,
    harris_k: float = 0.04,
    winsize: tuple[int, int] = (50, 50),
    nr_levels: int = 3,
    criteria: tuple = (3, 10, 0),
    flags: int = 0,
    min_eig_thr: float = 1e-4,
    nr_std_outlier: int = 3,
    k_outlier: int = 30,
) -> tuple[np.ndarray, np.ndarray]:
    """Sparse Lucas-Kanade motion vectors, the ``MaskedArray``-free fast path.

    A faithful reimplementation of ``pysteps.motion.lucaskanade.dense_lucaskanade``
    with ``dense=False``: for each consecutive image pair it removes small
    isolated echoes (morphological opening), detects Shi-Tomasi corners, tracks
    them with ``cv2.calcOpticalFlowPyrLK``, pools the vectors and removes
    outliers. Unlike pysteps it never builds a ``numpy.ma.MaskedArray`` (the
    dominant cost on mostly-no-data radar fields); the no-data mask is derived
    once per frame as a plain boolean. Default parameters match pysteps.

    ``images`` is a ``(T, m, n)`` array (oldest first) with non-finite values
    marking no-data. Returns ``(xy, uv)`` sparse vectors, matching the tuple
    returned by ``dense_lucaskanade(..., dense=False)``.
    """
    import cv2

    # Outlier removal operates on the small sparse-vector arrays, so pysteps'
    # implementation is reused directly (it is not a hotspot).
    from pysteps.utils.cleansing import detect_outliers

    images = np.asarray(images)
    detect_kwargs = dict(
        maxCorners=max_corners,
        qualityLevel=quality_level,
        minDistance=min_distance,
        blockSize=block_size,
        useHarrisDetector=use_harris,
        k=harris_k,
    )
    lk_kwargs = dict(
        winSize=tuple(winsize),
        maxLevel=nr_levels,
        criteria=criteria,
        flags=flags,
        minEigThreshold=min_eig_thr,
    )

    xy_list: list[np.ndarray] = []
    uv_list: list[np.ndarray] = []
    for n in range(images.shape[0] - 1):
        prvs = _prepare_frame(images[n], size_opening)
        nxt = _prepare_frame(images[n + 1], size_opening)
        if prvs is None or nxt is None:
            continue
        prvs_filled, prvs_valid, prvs_vmin = prvs
        next_filled, next_valid, next_vmin = nxt

        prvs_u8 = _frame_uint8(prvs_filled, prvs_valid, prvs_vmin)
        points = _detect_points(prvs_u8, prvs_valid, buffer_mask=buffer_mask, detect_kwargs=detect_kwargs)
        if points.shape[0] == 0:
            continue

        next_u8 = _frame_uint8(next_filled, next_valid, next_vmin)
        p0 = np.copy(points)
        p1, status, _err = cv2.calcOpticalFlowPyrLK(prvs_u8, next_u8, p0, None, **lk_kwargs)

        status = np.atleast_1d(status.squeeze()) == 1
        if not np.any(status):
            continue
        xy_list.append(p0[status, :])
        uv_list.append(p1[status, :] - p0[status, :])

    if not xy_list:
        empty = np.empty((0, 2))
        return empty, empty

    xy = np.concatenate(xy_list, axis=0)
    uv = np.concatenate(uv_list, axis=0)

    outliers = detect_outliers(uv, nr_std_outlier, xy, k_outlier, False)
    return xy[~outliers, :], uv[~outliers, :]
