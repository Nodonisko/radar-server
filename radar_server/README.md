# radar_server

Multi-country radar input fetching, backfill, rendering, and runtime loop.

## Setup

### macOS Python

Use Homebrew Python 3.13 on macOS. Do not use the system `/usr/bin/python3`
for this server: it is Python 3.9 on this machine, while current `pysteps`
requires newer Python syntax.

Check the interpreter:

```bash
/opt/homebrew/bin/python3.13 --version
```

Install dependencies for that Python version:

```bash
/opt/homebrew/bin/python3.13 -m pip install --user --break-system-packages -r requirements.txt
```

Homebrew Python is an externally managed environment, so `--user
--break-system-packages` installs packages into the user site-packages without
writing into Homebrew's managed Python directory.

Optional PNG optimization needs `oxipng`:

```bash
brew install oxipng
```

Create local `.env`:

```bash
METEOGATE_API_KEY=...
```

## Commands

```bash
/opt/homebrew/bin/python3.13 -m radar_server run-once --no-optimize
/opt/homebrew/bin/python3.13 -m radar_server run --no-optimize
/opt/homebrew/bin/python3.13 -m radar_server poll --no-optimize
/opt/homebrew/bin/python3.13 -m radar_server mqtt --no-optimize
```

- `run-once`: one fetch/backfill/render cycle (`--no-forecast` to skip forecasts).
- `run`: queue-based runtime, MQTT primary + polling/backfill fallback.
- `poll`: legacy synchronous polling-only scheduler.
- `mqtt`: MQTT watcher only, downloads without rendering (debug tool).
- omit `--no-optimize` to use `oxipng`.

## Config

Main config is Python code in `radar_server/config.py`.

To export the product configurations (including ID, label, and geographic bounds) into a static JSON file for client apps, run from the repository root:

```bash
/opt/homebrew/bin/python3.13 export_products.py
```

This generates `products.json` based on the current configuration.

## Runtime Pipeline

```text
config
  -> fetching.py
  -> input_index.py
  -> queueing.py / workers.py
  -> render_jobs.py
  -> rendering/
  -> forecast_generation.py / forecast_store.py
  -> pruning.py
```

Runtime mode:

```text
MQTT / polling (main thread, networking only)
  -> ingest queue -> DownloadWorker (1 thread): download, refresh index,
     enqueue render tasks, record wanted forecasts
  -> render priority queue -> RenderWorker (1 thread, one render at a time)
  -> ForecastGenPool (2 threads, dispatched only while render lane is idle):
     pysteps motion + extrapolation -> fields to disk -> forecast render tasks
```

`run` starts MQTT and then immediately runs startup backfill.

## Concurrency

- All heavy work runs on worker threads; the main thread only orchestrates.
- Numeric priority, lower renders first: `cz=0`, countries `10`,
  `central_europe=20`, forecasts `1000+`. Observed frames always beat
  forecast frames.
- Forecast work coalesces to the latest issue time; stale generations are
  discarded.
- On startup the runtime reconciles pending work from the filesystem; on
  shutdown queued work is abandoned and rebuilt on next start.

## Forecasts

- `ForecastProduct` links to a parent `ProductConfig` and reuses its bounds,
  palette, and variants.
- Default: Lucas-Kanade motion from 3 history frames, lead times
  10–60 min in 10 min steps.
- Generated fields are written as `.npz` to `data/<parent>/forecast_fields/`
  (atomic clear-and-replace, latest issue only); rendering reads them like
  ordinary inputs, so it is idempotent and restart-durable.
- Frames render to `<parent output_dir>/forecast/<base>_fctNN_overlay.png`.

## Fetching

- CHMI uses HTTP directory listing.
- OPERA uses MeteoGate ORD API for polling/backfill.
- OPERA live updates use MQTT topic:
  `ORD/eu.eumetnet/0-20010-0-OPERA/DBZH`.
- API key is sent as header `apikey`.

## Backfill

- Startup backfill always runs.
- MQTT is primary for OPERA.
- Polling fallback runs when MQTT is disconnected/stale.
- Scheduled backfill runs even when MQTT is healthy.
- CHMI has no MQTT source; it keeps normal polling.

## Retention

Default retention is 2 hours:

```python
RetentionPolicy(keep_for_seconds=7200)
```

Retention applies to:

- input files in `radar_server/data/`
- output files in `radar_server/output/`
- forecast fields in `data/<parent>/forecast_fields/`
- local input index scan window
- polling lookback window

Set `keep_for_seconds=None` to disable retention for a specific input/product.

## Rendering

Rendering internals live in `radar_server/rendering/`.

Output per product/timestamp:

```text
<base>_overlay.png
<base>_overlay_small.png
<base>.json
```

The JSON sidecar contains bounds, variants, source files, palette, quantity, and CRS.

## Tests

```bash
/opt/homebrew/bin/python3.13 -m pytest radar_server/tests/
/opt/homebrew/bin/python3.13 -m pytest radar_server/rendering/tests/
```

## Decisions

- Inputs own file suffixes and retention.
- Products own output directory, bounds, render profile, and retention.
- Timestamps are exact; European radar products are assumed on a 5-minute grid.
- Filesystem is the source of truth; input index is rebuilt from disk before rendering.
- Startup scan uses wall-clock retention cutoff; old files are skipped.
- MQTT and polling feed the same filesystem-index/render path.
- OPERA national neighbors use the European composite because national DBZH composites were not confirmed in ORD.
- Cropping uses `render_composite_png(..., bounds=...)`, even for one source file.
