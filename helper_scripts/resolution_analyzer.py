#!/usr/bin/env python3
"""
Resolution Analysis Tool for Radar HDF5 Data
Analyzes optimal PNG resolutions and DPI settings based on HDF5 data dimensions
"""
import h5py
import numpy as np
from png_generator import print_resolution_analysis, calculate_optimal_png_resolution

def analyze_hdf5_file(filename):
    """
    Analyze HDF5 file and print comprehensive resolution recommendations
    
    Args:
        filename: Path to HDF5 radar file
    """
    print(f"Analyzing HDF5 file: {filename}")
    print("="*80)
    
    with h5py.File(filename, 'r') as f:
        # Get geographic information
        where_group = f['where']
        xsize = where_group.attrs['xsize']
        ysize = where_group.attrs['ysize']
        ll_lat = where_group.attrs['LL_lat']
        ll_lon = where_group.attrs['LL_lon']
        ur_lat = where_group.attrs['UR_lat'] 
        ur_lon = where_group.attrs['UR_lon']
        
        # Get actual data shape
        data_group = f['dataset1/data1']
        raw_data = data_group['data'][...]
        
        # Extract metadata
        date = f['what'].attrs['date'].decode('utf-8')
        time = f['what'].attrs['time'].decode('utf-8')
        
        print(f"📄 FILE INFORMATION:")
        print(f"  File: {filename}")
        print(f"  Date/Time: {date} {time} UTC")
        print(f"  Geographic bounds: ({ll_lat:.3f}°, {ll_lon:.3f}°) to ({ur_lat:.3f}°, {ur_lon:.3f}°)")
        print(f"  Coverage area: {ur_lon - ll_lon:.3f}° × {ur_lat - ll_lat:.3f}°")
        
        print(f"\n📊 DATA DIMENSIONS:")
        print(f"  HDF attributes: xsize={xsize}, ysize={ysize}")
        print(f"  Actual data shape: {raw_data.shape}")
        print(f"  Data type: {raw_data.dtype}")
        
        # Use actual data dimensions (should match HDF attributes)
        hdf_height, hdf_width = raw_data.shape
        
        # Verify dimensions match
        if hdf_width != xsize or hdf_height != ysize:
            print(f"  ⚠️  WARNING: Data shape ({hdf_width}×{hdf_height}) doesn't match HDF attributes ({xsize}×{ysize})")
        
        # Print comprehensive resolution analysis
        print_resolution_analysis(hdf_width, hdf_height)
        
        # Calculate geographic resolution
        lon_range = ur_lon - ll_lon
        lat_range = ur_lat - ll_lat
        km_per_pixel_lon = (lon_range * 111.32) / hdf_width  # Approximate km per degree longitude
        km_per_pixel_lat = (lat_range * 110.54) / hdf_height  # Approximate km per degree latitude
        
        print(f"\n🌍 GEOGRAPHIC RESOLUTION:")
        print(f"{'='*60}")
        print(f"  Longitude coverage: {lon_range:.3f}° ({lon_range * 111.32:.1f} km)")
        print(f"  Latitude coverage: {lat_range:.3f}° ({lat_range * 110.54:.1f} km)")
        print(f"  Resolution per pixel:")
        print(f"    Longitude: ~{km_per_pixel_lon:.2f} km/pixel")
        print(f"    Latitude: ~{km_per_pixel_lat:.2f} km/pixel")
        print(f"    Average: ~{(km_per_pixel_lon + km_per_pixel_lat)/2:.2f} km/pixel")
        
        # Return the recommendations for potential use
        return calculate_optimal_png_resolution(hdf_width, hdf_height)

def demonstrate_resolution_mapping():
    """
    Demonstrate different resolution mapping scenarios
    """
    print("\n" + "="*80)
    print("RESOLUTION MAPPING DEMONSTRATION")
    print("="*80)
    
    # Example with Czech radar data dimensions
    hdf_width, hdf_height = 598, 378
    
    print(f"\n🎯 For radar data with {hdf_width}×{hdf_height} pixels:")
    
    # Show what happens with different DPI settings for exact mapping
    dpis = [72, 96, 150, 200, 300]
    
    print(f"\n📐 EXACT 1:1 MAPPING at different DPIs:")
    print(f"{'-'*50}")
    for dpi in dpis:
        figsize_w = hdf_width / dpi
        figsize_h = hdf_height / dpi
        print(f"  {dpi:3d} DPI: {hdf_width}×{hdf_height} pixels = {figsize_w:.2f}\"×{figsize_h:.2f}\" figure")
    
    # Show file size implications
    print(f"\n💾 ESTIMATED FILE SIZES (approximate):")
    print(f"{'-'*50}")
    base_size = (hdf_width * hdf_height) / 1000  # Rough estimate in KB
    print(f"  Exact 1:1 (598×378):     ~{base_size:.0f} KB")
    print(f"  2x resolution (1196×756): ~{base_size*4:.0f} KB")
    print(f"  Web medium (800×504):     ~{(800*504)/1000:.0f} KB")
    print(f"  Print quality (1600×1008): ~{(1600*1008)/1000:.0f} KB")

if __name__ == "__main__":
    # Analyze the sample HDF5 file
    radar_file = "T_PABV23_C_OKPR_20250913162500.hdf"
    
    if __name__ == "__main__":
        import os
        if os.path.exists(radar_file):
            recommendations = analyze_hdf5_file(radar_file)
            demonstrate_resolution_mapping()
            
            print(f"\n" + "="*80)
            print("SUMMARY & NEXT STEPS")
            print("="*80)
            print(f"✅ Analysis complete for {radar_file}")
            print(f"📊 Found {len(recommendations['configurations'])} optimal resolution configurations")
            print(f"🎯 Recommended: Use exact 1:1 mapping (598×378 pixels) for perfect data fidelity")
            print(f"🌐 For web use: 800×504 pixels provides good balance of quality and file size")
            print(f"🖨️  For print: 1600×1008 pixels at 200 DPI gives high-quality output")
            
        else:
            print(f"❌ Error: File {radar_file} not found!")
            print("Please make sure the HDF5 radar file is in the current directory.")

