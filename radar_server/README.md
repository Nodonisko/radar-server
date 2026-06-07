# radar_server

Multi-country radar input fetching, backfill, rendering, and runtime loop.

## Setup

```bash
python3 -m pip install -r requirements.txt
```

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
python3 -m radar_server run-once --no-optimize
python3 -m radar_server run --no-optimize
python3 -m radar_server poll --no-optimize
python3 -m radar_server mqtt --no-optimize
```

- `run-once`: one fetch/backfill/render cycle.
- `run`: MQTT primary + polling/backfill fallback.
- `poll`: polling-only runtime.
- `mqtt`: MQTT-only runtime.
- omit `--no-optimize` to use `oxipng`.

## Config

Main config is Python code in `radar_server/config.py`.

To export the product configurations (including ID, label, and geographic bounds) into a static JSON file for client apps, run from the repository root:

```bash
python3 export_products.py
```

This generates `products.json` based on the current configuration.

## Runtime Pipeline

```text
config
  -> fetching.py
  -> input_index.py
  -> render_jobs.py
  -> rendering/
  -> pruning.py
```

Runtime mode:

```text
MQTT notification -> download file -> input index -> render ready products -> prune
polling backfill  -> download files -> input index -> render ready products -> prune
```

`run` starts MQTT and then immediately runs startup backfill.

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
python3 -m pytest radar_server/tests/
python3 -m pytest radar_server/rendering/tests/
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
