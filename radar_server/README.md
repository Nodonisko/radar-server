# radar_server

v2 of the radar HDF5 → PNG processor. Country-agnostic rewrite of `../new_version/`
(which was Czech/CHMI only).

## Features

- ODIM HDF5 → PNG, reprojected to Web Mercator with a lat/lon bbox sidecar.
- Lossless output resolution computed per file (finest native cell).
- Smaller variants via max-pooling (fractional factors supported).
- Composites: merge N HDF5 files into one PNG, custom or union bounds, max overlap.
- Paletted PNGs (one transparent index) + oxipng.

## Pipeline

```
decode → reproject (lossless Web Mercator) → [downsample] → colorize → encode (PNG + JSON)
```

## Decisions (and the non-obvious why)

- **Reproject everything to Web Mercator (EPSG:3857), emit a lat/lon bbox.** Web
  maps are all Mercator, so one bbox image-overlay places any country's output
  correctly. CHMI is *already* Mercator (its `projdef` is spherical merc), so the
  CZ warp is a pixel-exact identity; only other projections actually resample.

- **Lossless resolution = finest source cell projected into Mercator.** Never
  drops a native cell. Non-Mercator sources gain pixels because Mercator's
  `sec(lat)` stretch varies across the grid (modest for mid-lat Europe, ~1.6× for
  Norway, severe only past ~78°N). No `native`/override policy — lossless only.

- **No matplotlib.** It only colorized, and its `bbox_inches="tight"` silently
  downscaled output to ~77% of the native grid (v1 `overlay` was 463×291, not
  598×378). A numpy LUT + Pillow gives exact, deterministic, indexed PNGs.

- **Smaller variants use max-pooling, not averaging.** dBZ is logarithmic and the
  source is a MAX-Z product, so averaging hides storm cores; max preserves peaks.
  The variant factor may be fractional (e.g. 1.5) via `np.fmax.reduceat`.

- **No upsample / retina (2×) variant.** No reason to upsample a lossless image;
  smaller variants go the other way (downsample).

- **Composites build one target grid, then warp each input in and `fmax`.** Same
  cell centres → no mosaic seams; overlap resolves to the max echo. Grid res =
  finest input; extent = custom bounds or union. Inputs **must share a timestamp**
  (error otherwise); the caller supplies the full filename `base`.

- **Sentinels:** nodata → `NaN`, clear-sky (`undetect`) → `-inf`; both render
  transparent regardless of palette floor.

