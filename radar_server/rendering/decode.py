"""ODIM HDF5 decoder — produces a country-agnostic :class:`RadarField`.

This is the input adapter at the front of the pipeline. It reads the ODIM
``where`` group for georeferencing (``projdef`` + corner lat/lons + scale) so
the field carries its real CRS, instead of assuming a plain lat/lon box. The
country abstraction will later select decoders/quantities per source; for now
this handles the OPERA/ODIM composites the project already consumes.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
from pyproj import Transformer

from .core import GeoTransform, RadarField

# Products that represent a 2D composite we can render directly.
_COMPOSITE_PRODUCTS = ("MAX", "MAXZ", "COMP", "SURF")


def _decode_attr(value) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _select_dataset(hdf: h5py.File, quantity: str) -> str:
    """Pick the dataset group matching ``quantity`` (ODIM has datasetN/data1).

    Prefers a composite product (MAX/COMP/...). Raises if no dataset carries the
    requested quantity, rather than silently using a different one: returning a
    mismatched dataset would mislabel the field and apply the wrong palette.
    """
    datasets = sorted(k for k in hdf.keys() if k.startswith("dataset"))
    available: list[str] = []
    fallback: str | None = None
    for name in datasets:
        actual = _decode_attr(hdf[f"{name}/data1/what"].attrs["quantity"])
        available.append(actual)
        if actual != quantity:
            continue
        if fallback is None:
            fallback = name
        product = _decode_attr(hdf[f"{name}/what"].attrs.get("product", b""))
        if product in _COMPOSITE_PRODUCTS:
            return name
    if fallback is not None:
        return fallback
    raise ValueError(
        f"no dataset with quantity {quantity!r} in {hdf.filename!r}; "
        f"available quantities: {sorted(set(available))}"
    )


def load_odim_hdf(path: Path, quantity: str = "DBZH") -> RadarField:
    with h5py.File(path, "r") as hdf:
        what = hdf["what"].attrs
        timestamp = datetime.strptime(
            f"{_decode_attr(what['date'])}{_decode_attr(what['time'])}",
            "%Y%m%d%H%M%S",
        )

        where = hdf["where"].attrs
        projdef = _decode_attr(where["projdef"])
        xsize, ysize = int(where["xsize"]), int(where["ysize"])
        xscale, yscale = float(where["xscale"]), float(where["yscale"])
        ul_lon, ul_lat = float(where["UL_lon"]), float(where["UL_lat"])

        dataset = _select_dataset(hdf, quantity)
        group = hdf[f"{dataset}/data1"]
        raw = group["data"][...]
        meta = group["what"].attrs
        gain = float(meta["gain"])
        offset = float(meta["offset"])
        nodata = float(meta["nodata"])
        undetect = float(meta["undetect"])

    # Project the top-left grid corner into the source CRS to anchor the grid.
    to_src = Transformer.from_crs("EPSG:4326", projdef, always_xy=True)
    x_min, y_max = to_src.transform(ul_lon, ul_lat)
    transform = GeoTransform(x_min=x_min, y_max=y_max, px=xscale, py=yscale, width=xsize, height=ysize)

    values = np.full(raw.shape, np.nan, dtype=np.float32)
    valid = (raw != nodata) & (raw != undetect)
    values[valid] = raw[valid] * gain + offset
    # Clear-sky ("undetect"): force below any palette floor so it renders
    # transparent regardless of quantity or palette.
    values[raw == undetect] = -np.inf

    return RadarField(values=values, crs=projdef, transform=transform, quantity=quantity, timestamp=timestamp)
