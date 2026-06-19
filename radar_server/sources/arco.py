"""Discover and materialize radar frames from a MeteoHub ARCO Zarr store.

The ARCO store exposes a cloud-optimized Zarr array over HTTP. We read the
consolidated metadata to map chunk indices to timestamps, fetch the Blosc-encoded
data chunk for a frame, and write it out as a minimal ODIM HDF5 file so the rest
of the pipeline can treat it like any other locally materialized input.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests import RequestException

from ..config import ArcoZarrSource, InputConfig
from .base import (
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    TIMEOUT_SECONDS,
    FetchError,
    LocalInputFile,
    RateLimitedError,
    RemoteInputFile,
    SkippableFetchError,
    request,
    request_json,
)

LOGGER = logging.getLogger(__name__)

# Cache of the (static) ARCO time axis per store URL. The axis is pre-allocated
# years into the future and never changes; only the data chunks fill in, so one
# fetch per process is enough to map a chunk index to its valid timestamp.
_ARCO_TIME_AXIS_CACHE: dict[str, "object"] = {}


def discover(
    input_config: InputConfig,
    source: ArcoZarrSource,
    *,
    now: datetime | None,
    limit: int | None,
) -> list[RemoteInputFile]:
    """Discover recent ARCO frames as materializable :class:`RemoteInputFile`s.

    Reads the consolidated metadata for the data frontier (``last_valid``) and
    enumerates that frame plus the preceding ones, newest-first. Frames are not
    probed here; a missing chunk surfaces as a skippable error at download time.
    """

    import numpy as np

    auth = _arco_auth(source)
    metadata = _arco_metadata(source, auth)
    last_valid = metadata.get(".zattrs", {}).get("last_valid")
    if not last_valid:
        LOGGER.warning("ARCO source %s has no last_valid attribute; nothing to fetch", source.id)
        return []

    georef = _arco_georef(metadata, source.variable)
    axis = _arco_time_axis(source, auth)
    lv = np.datetime64(last_valid, "m")
    frontier = int(np.searchsorted(axis, lv))
    if frontier >= axis.size:
        frontier = axis.size - 1
    while frontier > 0 and axis[frontier] > lv:
        frontier -= 1

    window = limit if limit is not None else _arco_window(input_config)
    window = max(1, window)

    files: list[RemoteInputFile] = []
    for offset in range(window):
        idx = frontier - offset
        if idx < 0:
            break
        timestamp = axis[idx].astype("datetime64[s]").astype(datetime)
        files.append(
            RemoteInputFile(
                input=input_config,
                timestamp=timestamp,
                url=_arco_chunk_url(source, idx),
                filename=f"{input_config.id}_{timestamp:%Y%m%d%H%M%S}.h5",
                metadata={"arco_index": idx, "arco_georef": georef, "arco_dtype": georef["dtype"]},
            )
        )
    return files


def download_frame(remote: RemoteInputFile, destination: Path) -> LocalInputFile:
    import numpy as np
    from numcodecs import Blosc

    source = remote.input.source
    assert isinstance(source, ArcoZarrSource)
    georef = remote.metadata["arco_georef"]
    response = _arco_get(remote.url, _arco_auth(source))
    raw = Blosc().decode(response.content)
    values = np.frombuffer(raw, dtype=remote.metadata["arco_dtype"]).reshape(georef["ysize"], georef["xsize"])

    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_name(f"{destination.name}.part")
    LOGGER.info("Materializing ARCO frame %s -> %s", remote.url, destination)
    _write_odim_sri(tmp_path, values, georef, remote.timestamp)
    tmp_path.replace(destination)
    return LocalInputFile(remote.input, remote.timestamp, destination, remote, downloaded=True)


def _write_odim_sri(path: Path, values, georef: dict[str, Any], timestamp: datetime) -> None:
    """Write a minimal ODIM HDF5 the project decoder understands.

    The ARCO ``RR`` field is a precipitation rate in mm/h with NaN outside
    coverage. We store it with gain=1/offset=0 and a ``nodata`` sentinel for
    NaN; the ``undetect`` sentinel is left unused (dry 0.0 cells fall below the
    palette floor and render transparent anyway).
    """

    import h5py
    import numpy as np

    nodata = -9999.0
    undetect = -8888.0
    data = np.where(np.isfinite(values), values, nodata).astype("float32")

    with h5py.File(path, "w") as hdf:
        what = hdf.create_group("what")
        what.attrs["object"] = "COMP"
        what.attrs["date"] = timestamp.strftime("%Y%m%d")
        what.attrs["time"] = timestamp.strftime("%H%M%S")

        where = hdf.create_group("where")
        where.attrs["projdef"] = georef["projdef"]
        where.attrs["xsize"] = int(georef["xsize"])
        where.attrs["ysize"] = int(georef["ysize"])
        where.attrs["xscale"] = float(georef["xscale"])
        where.attrs["yscale"] = float(georef["yscale"])
        where.attrs["UL_lon"] = float(georef["UL_lon"])
        where.attrs["UL_lat"] = float(georef["UL_lat"])

        dataset = hdf.create_group("dataset1")
        dataset.create_group("what").attrs["product"] = "SURF"
        data1 = dataset.create_group("data1")
        data1.create_dataset("data", data=data, compression="gzip", compression_opts=4)
        meta = data1.create_group("what")
        meta.attrs["quantity"] = "RATE"
        meta.attrs["gain"] = 1.0
        meta.attrs["offset"] = 0.0
        meta.attrs["nodata"] = nodata
        meta.attrs["undetect"] = undetect


def _arco_metadata(source: ArcoZarrSource, auth: tuple[str, str]) -> dict[str, Any]:
    payload = request_json(f"{source.store_url}/.zmetadata", auth=auth)
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise FetchError(f"ARCO store {source.store_url!r} returned no consolidated metadata")
    return metadata


def _arco_georef(metadata: dict[str, Any], variable: str) -> dict[str, Any]:
    crs = metadata.get("crs/.zattrs")
    array = metadata.get(f"{variable}/.zarray")
    if not isinstance(crs, dict) or not isinstance(array, dict):
        raise FetchError("ARCO metadata missing crs attributes or variable array spec")
    return {
        "projdef": str(crs["proj4"]),
        "xsize": int(crs["where_xsize"]),
        "ysize": int(crs["where_ysize"]),
        "xscale": float(crs["where_xscale"]),
        "yscale": float(crs["where_yscale"]),
        "UL_lon": float(crs["where_UL_lon"]),
        "UL_lat": float(crs["where_UL_lat"]),
        "dtype": str(array["dtype"]),
        "variable": variable,
    }


def _arco_time_axis(source: ArcoZarrSource, auth: tuple[str, str]):
    import numpy as np
    from numcodecs import Blosc

    cached = _ARCO_TIME_AXIS_CACHE.get(source.store_url)
    if cached is not None:
        return cached
    response = request("GET", f"{source.store_url}/time/0", auth=auth)
    decoded = Blosc().decode(response.content)
    axis = np.frombuffer(decoded, dtype="<i8").view("<M8[ns]").astype("datetime64[m]")
    _ARCO_TIME_AXIS_CACHE[source.store_url] = axis
    return axis


def _arco_chunk_url(source: ArcoZarrSource, index: int) -> str:
    # 3D array (time, y, x) with one chunk per timestep and "." separators.
    return f"{source.store_url}/{source.variable}/{index}.0.0"


def _arco_window(input_config: InputConfig) -> int:
    keep_for = input_config.retention.keep_for_seconds
    period = max(1, input_config.source.polling.expected_period_seconds)
    if keep_for is None:
        return 12
    return max(1, int(keep_for // period) + 2)


def _arco_auth(source: ArcoZarrSource) -> tuple[str, str]:
    username, key = source.credentials()
    if not username or not key:
        raise FetchError(
            f"ARCO source {source.id!r} missing credentials; set ARCO_USERNAME and ARCO_ACCESS_KEY"
        )
    return username, key


def _arco_get(url: str, auth: tuple[str, str]) -> requests.Response:
    """GET an ARCO chunk; raise :class:`SkippableFetchError` if it isn't there."""

    last_error: RequestException | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT_SECONDS, auth=auth)
        except RequestException as exc:
            last_error = exc
            LOGGER.warning("ARCO GET failed for %s (attempt %d/%d): %s", url, attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_DELAY_SECONDS * attempt)
            continue
        if response.status_code == 404:
            raise SkippableFetchError(f"ARCO chunk not available: {url!r}")
        if response.status_code == 429:
            raise RateLimitedError(f"rate limited by ARCO store for {url!r}")
        try:
            response.raise_for_status()
        except RequestException as exc:
            last_error = exc
            LOGGER.warning("ARCO GET %s returned %s (attempt %d/%d)", url, response.status_code, attempt, MAX_RETRIES)
            time.sleep(RETRY_DELAY_SECONDS * attempt)
            continue
        return response
    raise FetchError(f"giving up on ARCO chunk {url!r} after {MAX_RETRIES} attempts") from last_error
