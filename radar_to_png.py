#!/usr/bin/env python3
"""
Convert Czech Meteo Radar HDF5 data to PNG image
Processes MAX_Z (maximum radar reflectivity) data from CHMI radar composite
"""
import h5py
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from datetime import datetime
import os

def convert_radar_value(raw_value, gain, offset, nodata, undetect):
    """
    Convert raw radar data to dBZ (decibels relative to Z)
    According to ODIM HDF5 specification: value = raw * gain + offset
    """
    # Handle special values
    if raw_value == nodata:
        return np.nan  # No data
    if raw_value == undetect:
        return -32.0   # Below detection threshold
    
    # Convert to physical value (dBZ)
    return raw_value * gain + offset

def create_radar_colormap():
    """
    Create a colormap using the exact original colors with discrete 4 dBZ bands
    No interpolation between colors - each band uses a single color
    Values below 0 dBZ are transparent/not visible
    """

    colors_list_original = [
        '#390071', # 4-8 dBZ
        '#3001A9', # 8-12 dBZ
        '#0200FB', # 12-16 dBZ
        '#076CBC', # 16-20 dBZ
        '#00A400', # 20-24 dBZ
        '#00BB03', # 24-28 dBZ
        '#36D700', # 28-32 dBZ
        '#9CDD07', # 32-36 dBZ
        '#E0DC01', # 36-40 dBZ
        '#FBB200', # 40-44 dBZ
        '#F78600', # 44-48 dBZ
        '#FF5400', # 48-52 dBZ
        '#FE0100', # 52-56 dBZ
        '#A40003', # 56-60 dBZ
        '#FCFCFC'  # 60-64 dBZ
    ]

    # Use the original color list with discrete 4 dBZ bands
    # Create custom colormap with no interpolation
    cmap = colors.ListedColormap(colors_list_original)
    
    # Define boundaries for discrete color bands (4 dBZ intervals)
    # Values below 4 dBZ will be transparent (not visible)
    # Shifted scale: 4-8, 8-12, 12-16, ..., 60-64 dBZ
    bounds = [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64]
    norm = colors.BoundaryNorm(bounds, cmap.N)
    
    return cmap, norm

def process_radar_hdf5(filename, output_filename=None):
    """
    Process HDF5 radar file and generate PNG image
    """
    print(f"Processing radar file: {filename}")
    
    with h5py.File(filename, 'r') as f:
        # Extract metadata
        date = f['what'].attrs['date'].decode('utf-8')
        time = f['what'].attrs['time'].decode('utf-8')
        
        # Get geographic information
        where_group = f['where']
        ll_lat = where_group.attrs['LL_lat']
        ll_lon = where_group.attrs['LL_lon']
        ur_lat = where_group.attrs['UR_lat'] 
        ur_lon = where_group.attrs['UR_lon']
        xsize = where_group.attrs['xsize']
        ysize = where_group.attrs['ysize']
        
        print(f"Data timestamp: {date} {time} UTC")
        print(f"Geographic bounds: ({ll_lat:.3f}, {ll_lon:.3f}) to ({ur_lat:.3f}, {ur_lon:.3f})")
        print(f"Grid size: {xsize} x {ysize}")
        
        # Extract MAX reflectivity data (dataset1)
        data_group = f['dataset1/data1']
        raw_data = data_group['data'][...]
        
        # Get conversion parameters
        what_attrs = data_group['what'].attrs
        gain = what_attrs['gain']
        offset = what_attrs['offset'] 
        nodata = what_attrs['nodata']
        undetect = what_attrs['undetect']
        
        print(f"Data conversion: gain={gain}, offset={offset}, nodata={nodata}, undetect={undetect}")
        print(f"Raw data shape: {raw_data.shape}, dtype: {raw_data.dtype}")
        print(f"Raw data range: {raw_data.min()} to {raw_data.max()}")
        
        # Convert raw data to physical values (dBZ)
        # Vectorized conversion
        reflectivity = np.full(raw_data.shape, np.nan, dtype=np.float32)
        
        # Valid data points (not nodata or undetect)
        valid_mask = (raw_data != nodata) & (raw_data != undetect)
        reflectivity[valid_mask] = raw_data[valid_mask] * gain + offset
        
        # Set undetect values to minimum detectable value
        undetect_mask = (raw_data == undetect)
        reflectivity[undetect_mask] = -32.0
        
        print(f"Converted reflectivity range: {np.nanmin(reflectivity):.1f} to {np.nanmax(reflectivity):.1f} dBZ")
        print(f"Valid data points: {np.sum(valid_mask | undetect_mask)} / {raw_data.size}")
        
        # Create the visualization
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Create colormap
        cmap, norm = create_radar_colormap()
        
        # Mask values below 4 dBZ to make them transparent/not visible
        masked_reflectivity = np.copy(reflectivity)
        masked_reflectivity[masked_reflectivity < 4] = np.nan
        
        # Display the data with North at the top
        # The data array has North at the top (first row), so use origin='upper'
        im = ax.imshow(masked_reflectivity, 
                      extent=[ll_lon, ur_lon, ur_lat, ll_lat],
                      cmap=cmap, norm=norm, 
                      interpolation='nearest',
                      aspect='auto',
                      origin='upper')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
        cbar.set_label('Radar Reflectivity (dBZ)', fontsize=12)
        
        # Set labels and title
        ax.set_xlabel('Longitude (°E)', fontsize=12)
        ax.set_ylabel('Latitude (°N)', fontsize=12)
        
        # Format timestamp for title
        dt = datetime.strptime(f"{date}{time}", "%Y%m%d%H%M%S")
        title = f"Czech Radar Composite - Maximum Reflectivity (MAX_Z)\n{dt.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        ax.set_title(title, fontsize=14, fontweight='bold')
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Set reasonable tick intervals
        ax.set_xticks(np.arange(np.ceil(ll_lon), np.floor(ur_lon) + 1, 2))
        ax.set_yticks(np.arange(np.ceil(ll_lat), np.floor(ur_lat) + 1, 1))
        
        # Add data source info
        ax.text(0.02, 0.02, 'Data source: CHMI (Czech Hydrometeorological Institute)\nRadars: Brdy-Praha, Skalky', 
                transform=ax.transAxes, fontsize=8, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
        
        plt.tight_layout()
        
        # Save the image
        if output_filename is None:
            # Generate output filename based on input
            base_name = os.path.splitext(os.path.basename(filename))[0]
            output_filename = f"{base_name}_radar_maxz.png"
        
        plt.savefig(output_filename, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        print(f"Saved radar image: {output_filename}")
        
        # Generate pure radar image without any text or background
        pure_output = output_filename.replace('.png', '_pure.png')
        
        # Create figure for pure image - no axes, labels, or decorations
        fig_pure, ax_pure = plt.subplots(figsize=(12, 8))
        
        # Remove all axes, ticks, labels, and borders
        ax_pure.set_xticks([])
        ax_pure.set_yticks([])
        ax_pure.set_xticklabels([])
        ax_pure.set_yticklabels([])
        ax_pure.axis('off')
        
        # Display only the radar data
        im_pure = ax_pure.imshow(masked_reflectivity, 
                                extent=[ll_lon, ur_lon, ur_lat, ll_lat],
                                cmap=cmap, norm=norm, 
                                interpolation='nearest',
                                aspect='auto',
                                origin='upper')
        
        # Remove any padding/margins
        plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
        
        # Save with transparent background
        plt.savefig(pure_output, dpi=300, bbox_inches='tight', pad_inches=0,
                   facecolor='none', edgecolor='none', transparent=True)
        print(f"Saved pure radar image: {pure_output}")
        
        plt.show()
        
        return reflectivity, (ll_lon, ur_lon, ll_lat, ur_lat)

if __name__ == "__main__":
    # Process the radar file
    radar_file = "T_PABV23_C_OKPR_20250913162500.hdf"
    
    if os.path.exists(radar_file):
        reflectivity_data, bounds = process_radar_hdf5(radar_file)
        print("\nProcessing completed successfully!")
        print(f"Reflectivity statistics:")
        print(f"  Min: {np.nanmin(reflectivity_data):.1f} dBZ")
        print(f"  Max: {np.nanmax(reflectivity_data):.1f} dBZ") 
        print(f"  Mean: {np.nanmean(reflectivity_data):.1f} dBZ")
        print(f"  Coverage: {np.sum(~np.isnan(reflectivity_data)) / reflectivity_data.size * 100:.1f}%")
    else:
        print(f"Error: File {radar_file} not found!")
