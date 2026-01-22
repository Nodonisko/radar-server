# Radar HDF5 Processor

A Python application that downloads, processes, and serves Czech meteorological radar data from CHMI (Czech Hydrometeorological Institute).

## Features

- 📡 Downloads radar composite data (current + forecast) from CHMI OpenData
- 🗺️ Processes HDF5 files and generates PNG overlay images
- 🌐 Serves processed images via HTTP server
- ⏱️ Runs continuously with automatic scheduled updates (every 5 minutes)
- 🎨 Beautiful color-coded precipitation visualization
- 📊 Supports both standard and retina (2x) resolution outputs

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

## Installation

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Usage

Start the server from the project root:

```bash
python -m new_version
```

The application will:
1. Start an HTTP server on `http://localhost:8080`
2. Create necessary directories (`radar_data/`, `output/`, etc.)
3. Download radar data from CHMI
4. Process HDF5 files and generate PNG overlays
5. Run continuously, checking for new data every 5 minutes

## Configuration

Edit `new_version/config.py` to customize:

- **HTTP Server**: Host and port (default: `0.0.0.0:8080`)
- **Update Interval**: How often to check for new data (default: 300 seconds)
- **Storage Paths**: Where to store downloaded and processed files
- **Rendering**: DPI, colors, and parallel processing workers
- **Platform**: Enable/disable server on non-macOS systems

Key settings:
```python
DevServerConfig:
    host: "0.0.0.0"
    port: 8080
    enabled_only_on_macos: True  # Set to False for Linux/Windows

TimingConfig:
    publish_interval: 300  # seconds between updates
```

## Output

Generated files are saved to:
- `new_version/output/` - Current radar overlays
- `new_version/output_forecast/` - Forecast overlays (10-60 min ahead)

Each image is available in two resolutions:
- Standard: 72 DPI
- Retina: 144 DPI (2x)

## Data Sources

- **Current radar**: https://opendata.chmi.cz/meteorology/weather/radar/composite/maxz/hdf5/
- **Forecast radar**: https://opendata.chmi.cz/meteorology/weather/radar/composite/fct_maxz/hdf5/

## Project Structure

```
new_version/
├── __main__.py          # Application entry point
├── config.py            # Configuration settings
├── downloader.py        # Downloads HDF5 files from CHMI
├── hdf_reader.py        # Reads and parses HDF5 radar data
├── png_renderer.py      # Renders radar data as PNG overlays
├── png_pipeline.py      # Orchestrates rendering pipeline
├── http_server.py       # Static file HTTP server
├── scheduler.py         # Manages periodic data updates
├── forecast.py          # Forecast-specific processing
└── tests/               # Unit tests
```

## Development

Run tests:
```bash
python -m pytest new_version/tests/
```

## License

This project processes open data from the Czech Hydrometeorological Institute (CHMI).
