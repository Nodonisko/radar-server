"""Regenerate the real-data test fixtures from a EUMETNET OPERA composite.

The committed ``opera_*.h5`` files are small regional crops of a single OPERA
maximum-reflectivity composite (ODIM HDF5, Lambert Azimuthal Equal Area). They
exercise the real non-Mercator reprojection path with real precipitation, and
the two overlapping tiles drive the composite/merge tests.

To regenerate (free, no auth):

    curl -s "https://s3.waw3-1.cloudferro.com/openradar-24h/2026/06/05/OPERA/COMP/OPERA@20260605T0000@0@DBZH.h5" -o /tmp/opera.h5
    python radar_server/tests/fixtures/build_fixtures.py /tmp/opera.h5

Source: EUMETNET Open Radar Data, CC BY 4.0 (https://api.meteogate.eu).
"""

from __future__ import annotations

import sys
from pathlib import Path

import h5py
import numpy as np
from pyproj import Transformer

# Two overlapping windows over the rainy central-Europe region (Alps / S. Germany
# / Austria) of the source grid: west and east share a ~200 km band so the
# merge tests have genuine overlap. (row0, row1, col0, col1) in source pixels.
_TILES = {
    "opera_west.h5": (2620, 3180, 1750, 2200),
    "opera_east.h5": (2620, 3180, 2000, 2450),
}


def _attr(attrs, key):
    v = attrs[key]
    return v.decode() if isinstance(v, bytes) else v


def build(source: Path, out_dir: Path) -> None:
    with h5py.File(source, "r") as f:
        what = f["what"].attrs
        date, time = _attr(what, "date"), _attr(what, "time")
        where = f["where"].attrs
        projdef = _attr(where, "projdef")
        xscale, yscale = float(where["xscale"]), float(where["yscale"])
        ul_lon, ul_lat = float(where["UL_lon"]), float(where["UL_lat"])
        dwhat = f["dataset1/data1/what"].attrs
        gain, offset = float(dwhat["gain"]), float(dwhat["offset"])
        nodata, undetect = float(dwhat["nodata"]), float(dwhat["undetect"])
        quantity = _attr(dwhat, "quantity")
        raw = f["dataset1/data1/data"][...]

    to_laea = Transformer.from_crs("EPSG:4326", projdef, always_xy=True)
    to_wgs = Transformer.from_crs(projdef, "EPSG:4326", always_xy=True)
    x0, y0 = to_laea.transform(ul_lon, ul_lat)  # LAEA coords of the grid's UL corner

    for name, (r0, r1, c0, c1) in _TILES.items():
        tile = raw[r0:r1, c0:c1].astype(np.float32)
        tile_x0, tile_y0 = x0 + c0 * xscale, y0 - r0 * yscale
        tile_ul_lon, tile_ul_lat = to_wgs.transform(tile_x0, tile_y0)

        path = out_dir / name
        with h5py.File(path, "w") as g:
            g.attrs["Conventions"] = b"ODIM_H5/V2_4"
            gw = g.create_group("what")
            gw.attrs["date"] = date.encode()
            gw.attrs["time"] = time.encode()
            gw.attrs["object"] = b"COMP"
            gwh = g.create_group("where")
            gwh.attrs["projdef"] = projdef.encode()
            gwh.attrs["xsize"] = tile.shape[1]
            gwh.attrs["ysize"] = tile.shape[0]
            gwh.attrs["xscale"] = xscale
            gwh.attrs["yscale"] = yscale
            gwh.attrs["UL_lon"] = tile_ul_lon
            gwh.attrs["UL_lat"] = tile_ul_lat
            ds = g.create_group("dataset1")
            ds.create_group("what").attrs["product"] = b"MAX"
            d1 = ds.create_group("data1")
            d1.create_dataset("data", data=tile, compression="gzip", compression_opts=9)
            dw = d1.create_group("what")
            dw.attrs["gain"] = gain
            dw.attrs["offset"] = offset
            dw.attrs["nodata"] = nodata
            dw.attrs["undetect"] = undetect
            dw.attrs["quantity"] = quantity.encode()
        echoes = int(((tile != nodata) & (tile != undetect) & (tile >= 20)).sum())
        print(f"{name}: {tile.shape[1]}x{tile.shape[0]}  {path.stat().st_size // 1024} KB  strong_echoes={echoes}")


if __name__ == "__main__":
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/opera.h5")
    build(src, Path(__file__).resolve().parent)
