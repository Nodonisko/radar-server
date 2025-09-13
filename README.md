# Czech Meteorological Radar Data Visualization

This project converts Czech Hydrometeorological Institute (CHMI) radar data from HDF5 format to PNG images.

## Files Generated

- **T_PABV23_C_OKPR_20250913162500_radar_maxz.png** - Standard meteorological colormap
- **T_PABV23_C_OKPR_20250913162500_radar_maxz_high_contrast.png** - High-contrast version using turbo colormap

## Data Information

- **Source**: CHMI radar composite (Brdy-Praha and Skalky radars)
- **Product**: MAX_Z (Maximum radar reflectivity)
- **Timestamp**: 2025-09-13 16:25:00 UTC
- **Coverage**: Czech Republic territory
- **Resolution**: 1x1 km grid (598 x 378 pixels)
- **Geographic bounds**:
  - Longitude: 11.267° - 19.624° E
  - Latitude: 48.047° - 51.458° N
- **Projection**: Mercator (EPSG:3857) compatible with web maps

## Data Values

- **Range**: -32.0 to 61.5 dBZ (decibels relative to Z)
- **Coverage**: 99.3% valid data points
- **Mean reflectivity**: -29.6 dBZ

## Scripts

- **radar_to_png.py** - Main conversion script
- **examine_hdf5.py** - HDF5 file structure analysis
- **requirements.txt** - Python dependencies

## Usage

```bash
# Install dependencies
pip3 install -r requirements.txt

# Convert HDF5 to PNG
python3 radar_to_png.py
```

## Colormap Legend (Discrete 4 dBZ bands - Shifted Scale)

- **#390071** (4-8 dBZ): Very light precipitation
- **#3001A9** (8-12 dBZ): Light precipitation
- **#0200FB** (12-16 dBZ): Light rain
- **#076CBC** (16-20 dBZ): Light to moderate rain
- **#00A400** (20-24 dBZ): Moderate rain
- **#00BB03** (24-28 dBZ): Moderate rain
- **#36D700** (28-32 dBZ): Moderate to heavy rain
- **#9CDD07** (32-36 dBZ): Heavy rain
- **#E0DC01** (36-40 dBZ): Heavy rain
- **#FBB200** (40-44 dBZ): Very heavy rain
- **#F78600** (44-48 dBZ): Very heavy rain
- **#FF5400** (48-52 dBZ): Intense precipitation
- **#FE0100** (52-56 dBZ): Very intense precipitation
- **#A40003** (56-60 dBZ): Extreme precipitation
- **#FCFCFC** (60+ dBZ): Severe weather

**Note**: Values below 4 dBZ are not displayed (transparent)

## Technical Details

The script follows ODIM HDF5 v2.4 specification for radar data processing and converts raw uint8 values to physical reflectivity using the formula: `dBZ = raw_value * gain + offset`
