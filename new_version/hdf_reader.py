"""HDF5 loading and conversion utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Tuple

import h5py
import numpy as np


@dataclass(frozen=True)
class RadarMetadata:
    timestamp: datetime
    bounds: Tuple[float, float, float, float]  # lon_min, lon_max, lat_min, lat_max
    grid_shape: Tuple[int, int]
    nodata: int
    undetect: int


@dataclass(frozen=True)
class RadarProduct:
    data: np.ndarray
    metadata: RadarMetadata


def _decode_attr(attr) -> str:
    if isinstance(attr, bytes):
        return attr.decode("utf-8")
    return str(attr)


def load_radar_hdf(path: Path) -> RadarProduct:
    with h5py.File(path, "r") as dataset:
        date = _decode_attr(dataset["what"].attrs["date"])  # YYYYMMDD
        time = _decode_attr(dataset["what"].attrs["time"])  # HHMMSS
        timestamp = datetime.strptime(f"{date}{time}", "%Y%m%d%H%M%S")

        where = dataset["where"].attrs
        bounds = (float(where["LL_lon"]), float(where["UR_lon"]), float(where["LL_lat"]), float(where["UR_lat"]))
        xsize = int(where["xsize"])
        ysize = int(where["ysize"])

        data_group = dataset["dataset1/data1"]
        raw = data_group["data"][...]

        gain = float(data_group["what"].attrs["gain"])
        offset = float(data_group["what"].attrs["offset"])
        nodata = int(data_group["what"].attrs["nodata"])
        undetect = int(data_group["what"].attrs["undetect"])

    reflectivity = np.full(raw.shape, np.nan, dtype=np.float32)
    valid_mask = (raw != nodata) & (raw != undetect)
    reflectivity[valid_mask] = raw[valid_mask] * gain + offset
    reflectivity[raw == undetect] = -32.0

    metadata = RadarMetadata(
        timestamp=timestamp,
        bounds=bounds,
        grid_shape=(ysize, xsize),
        nodata=nodata,
        undetect=undetect,
    )

    return RadarProduct(data=reflectivity, metadata=metadata)


