#!/usr/bin/env python3
"""
Simple script to subset bands from tiles and write as COG.

Usage:
    # Using tile index CSV
    python subset_bands_to_cog.py --tile_index tiles.csv --bands 1,2,3 --output_dir ./output --output_nodata 0
    
    # Using direct file path
    python subset_bands_to_cog.py --input_file path/to/file.tif --bands 1,2,3 --output_dir ./output --output_nodata -9999
"""

import argparse
import pandas as pd
import rasterio
import numpy as np
import os
from pathlib import Path
import s3fs
import sys
sys.path.append('/projects/code/icesat2_boreal/lib')
from CovariateUtils import write_cog

def process_tile_bands(file_path, bands, output_path, band_names=None, 
                      target_crs=None, resolution=None, clip_geom=None, 
                      output_dtype=None, output_nodata=None, verbose=False):
    """
    Process a single tile: read specified bands and write as COG.
    
    Parameters:
    -----------
    file_path : str
        Path to input raster (local or S3)
    bands : list of int
        List of band numbers to extract (1-indexed)
    output_path : str
        Output file path
    band_names : list of str, optional
        Names for the output bands
    target_crs : str, optional
        Target CRS for reprojection
    resolution : tuple, optional
        Target resolution (x, y)
    clip_geom : dict, optional
        Geometry for clipping
    output_dtype : str, optional
        Output data type
    output_nodata : float, optional
        Output nodata value
    verbose : bool
        Enable verbose output
    """
    
    if verbose:
        print(f"Processing: {file_path}")
        print(f"  Bands: {bands}")
        print(f"  Output: {output_path}")
    
    # Read data using rasterio (handles both local and S3 paths)
    with rasterio.open(file_path) as src:
        # Validate bands
        max_band = src.count
        invalid_bands = [b for b in bands if b > max_band or b < 1]
        if invalid_bands:
            raise ValueError(f"Invalid bands {invalid_bands}. File has {max_band} bands.")
        
        # Read specified bands
        stack = src.read(bands)  # Returns (bands, height, width)
        
        # Get spatial info
        in_crs = src.crs
        src_transform = src.transform
        
        # Set band names
        if band_names is None:
            band_names = [f"band_{b}" for b in bands]
        elif len(band_names) != len(bands):
            raise ValueError(f"Number of band_names ({len(band_names)}) must match number of bands ({len(bands)})")
        
        # Handle input nodata
        input_nodata_value = src.nodata
        if input_nodata_value is None:
            # Try to infer from data type
            if src.dtypes[0] == 'uint8':
                input_nodata_value = 0
            else:
                input_nodata_value = -9999
        
        # Handle output nodata - use specified value or keep input nodata
        if output_nodata is not None:
            final_nodata = output_nodata
            # Convert input nodata to output nodata in the data
            if input_nodata_value is not None and input_nodata_value != output_nodata:
                stack = np.where(stack == input_nodata_value, output_nodata, stack)
        else:
            final_nodata = input_nodata_value
        
        if verbose:
            print(f"  Input shape: {stack.shape}")
            print(f"  Input CRS: {in_crs}")
            print(f"  Input nodata: {input_nodata_value}")
            print(f"  Output nodata: {final_nodata}")
    
    # Use write_cog from CovariateUtils
    write_cog(
        stack=stack,
        out_fn=output_path,
        in_crs=str(in_crs),
        src_transform=src_transform,
        bandnames=band_names,
        out_crs=target_crs,
        resolution=resolution,
        clip_geom=clip_geom,
        input_nodata_value=final_nodata,  # Pass the final nodata value
        resampling='nearest'
    )
    
    if verbose:
        print(f"  ✓ Completed: {output_path}")

def process_single_file(input_file, bands, output_dir, band_names=None,
                       output_prefix='processed', target_crs=None, 
                       resolution=None, output_nodata=None, verbose=False):
    """
    Process a single input file.
    """
    
    # Generate output filename
    input_name = Path(input_file).stem
    bands_str = ''.join(map(str, bands))
    output_filename = f"{output_prefix}_{input_name}_bands{bands_str}.tif"
    output_path = os.path.join(output_dir, output_filename)
    
    # Process the file
    process_tile_bands(
        file_path=input_file,
        bands=bands,
        output_path=output_path,
        band_names=band_names,
        target_crs=target_crs,
        resolution=resolution,
        output_nodata=output_nodata,
        verbose=verbose
    )
    
    return output_path

def process_tile_index(tile_index, bands, output_dir, s3_path_column='s3_path',
                      tile_id_column='tile_num', band_names=None, 
                      output_prefix='tile', target_crs=None, resolution=None,
                      output_nodata=None, tile_list=None, max_tiles=None, verbose=False):
    """
    Process files from a tile index CSV.
    """
    
    # Setup S3 filesystem for checking file existence
    fs = s3fs.S3FileSystem(anon=True)
    
    # Read tile index
    print(f"Reading tile index from: {tile_index}")
    df = pd.read_csv(tile_index)
    
    print(f"Loaded {len(df)} tiles from index")
    
    # Validate required columns
    if s3_path_column not in df.columns:
        raise ValueError(f"Column '{s3_path_column}' not found. Available: {list(df.columns)}")
    
    # Filter tiles if tile_list provided
    if tile_list is not None:
        if tile_id_column in df.columns:
            df = df[df[tile_id_column].isin(tile_list)]
            print(f"Filtered to {len(df)} tiles based on tile_list")
        else:
            print(f"Warning: {tile_id_column} column not found, ignoring tile_list filter")
    
    # Limit number of tiles if specified
    if max_tiles is not None:
        df = df.head(max_tiles)
        print(f"Limited to first {len(df)} tiles for processing")
    
    if len(df) == 0:
        print("No tiles to process!")
        return 0, 0
    
    # Process each tile
    print(f"\nProcessing {len(df)} tiles...")
    
    successful = 0
    failed = 0
    
    for idx, row in df.iterrows():
        file_path = row[s3_path_column]
        
        # Generate output filename
        if tile_id_column in df.columns:
            tile_id = row[tile_id_column]
            output_filename = f"{output_prefix}_{tile_id}_bands{''.join(map(str, bands))}.tif"
        else:
            output_filename = f"{output_prefix}_{idx:04d}_bands{''.join(map(str, bands))}.tif"
        
        output_path = os.path.join(output_dir, output_filename)
        
        # Skip if output already exists
        if os.path.exists(output_path):
            if verbose:
                print(f"Skipping (exists): {output_filename}")
            continue
        
        try:
            # Check if file exists (for S3 paths)
            if file_path.startswith('s3://') and not fs.exists(file_path):
                print(f"File not found: {file_path}")
                failed += 1
                continue
            
            # Process the tile
            process_tile_bands(
                file_path=file_path,
                bands=bands,
                output_path=output_path,
                band_names=band_names,
                target_crs=target_crs,
                resolution=resolution,
                output_nodata=output_nodata,
                verbose=verbose
            )
            
            successful += 1
            
            if not verbose:
                print(f"✓ {successful}/{len(df)}: {output_filename}")
                
        except Exception as e:
            print(f"✗ Error processing {file_path}: {e}")
            failed += 1
    
    return successful, failed

def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Extract bands from tiles and write as COG',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Input options - mutually exclusive
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--tile_index', type=str,
                           help='CSV file with tile index containing file paths')
    input_group.add_argument('--input_file', type=str,
                           help='Single input file path (local or S3)')
    
    # Required arguments
    parser.add_argument('--bands', required=True, type=str,
                       help='Comma-separated list of band numbers (1-indexed, e.g., "1,2,3")')
    parser.add_argument('--output_dir', required=True, type=str,
                       help='Output directory for COG files')
    
    # CSV-specific options
    parser.add_argument('--s3_path_column', type=str, default='s3_path',
                       help='Column name containing file paths (for CSV input)')
    parser.add_argument('--tile_id_column', type=str, default='tile_num',
                       help='Column name containing tile IDs (for CSV input)')
    parser.add_argument('--tile_list', type=str, default=None,
                       help='Comma-separated list of specific tile IDs to process (for CSV input)')
    parser.add_argument('--max_tiles', type=int, default=None,
                       help='Maximum number of tiles to process (for CSV input, useful for testing)')
    
    # General options
    parser.add_argument('--band_names', type=str, default=None,
                       help='Comma-separated list of band names (e.g., "red,green,blue")')
    parser.add_argument('--output_prefix', type=str, default='processed',
                       help='Prefix for output filenames')
    parser.add_argument('--target_crs', type=str, default=None,
                       help='Target CRS for reprojection (e.g., "EPSG:4326")')
    parser.add_argument('--resolution', nargs=2, type=float, default=None,
                       help='Target resolution as two values: x_res y_res')
    parser.add_argument('--output_nodata', type=float, default=None,
                       help='Output nodata value (e.g., 0, -9999, 255)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Parse bands
    try:
        bands = [int(b.strip()) for b in args.bands.split(',')]
    except ValueError:
        raise ValueError("Invalid bands format. Use comma-separated integers (e.g., '1,2,3')")
    
    # Parse band names if provided
    band_names = None
    if args.band_names:
        band_names = [name.strip() for name in args.band_names.split(',')]
    
    # Parse tile list if provided
    tile_list = None
    if args.tile_list:
        try:
            tile_list = [int(t.strip()) for t in args.tile_list.split(',')]
        except ValueError:
            # Try as strings if integers fail
            tile_list = [t.strip() for t in args.tile_list.split(',')]
    
    # Parse resolution
    resolution = None
    if args.resolution:
        resolution = tuple(args.resolution)
    
    print("=== Band Subset to COG Processor ===")
    if args.tile_index:
        print(f"Input: Tile index CSV - {args.tile_index}")
    else:
        print(f"Input: Single file - {args.input_file}")
    print(f"Bands to extract: {bands}")
    print(f"Output directory: {args.output_dir}")
    if band_names:
        print(f"Band names: {band_names}")
    if args.target_crs:
        print(f"Target CRS: {args.target_crs}")
    if resolution:
        print(f"Target resolution: {resolution}")
    if args.output_nodata is not None:
        print(f"Output nodata value: {args.output_nodata}")
    print()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    if args.input_file:
        # Process single file
        print("Processing single file...")
        try:
            output_path = process_single_file(
                input_file=args.input_file,
                bands=bands,
                output_dir=args.output_dir,
                band_names=band_names,
                output_prefix=args.output_prefix,
                target_crs=args.target_crs,
                resolution=resolution,
                output_nodata=args.output_nodata,
                verbose=args.verbose
            )
            print(f"✓ Successfully processed: {output_path}")
            
        except Exception as e:
            print(f"✗ Error processing {args.input_file}: {e}")
            return 1
    
    else:
        # Process tile index
        try:
            successful, failed = process_tile_index(
                tile_index=args.tile_index,
                bands=bands,
                output_dir=args.output_dir,
                s3_path_column=args.s3_path_column,
                tile_id_column=args.tile_id_column,
                band_names=band_names,
                output_prefix=args.output_prefix,
                target_crs=args.target_crs,
                resolution=resolution,
                output_nodata=args.output_nodata,
                tile_list=tile_list,
                max_tiles=args.max_tiles,
                verbose=args.verbose
            )
            
            print(f"\n=== Processing Complete ===")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print(f"Total processed: {successful + failed}")
            
        except Exception as e:
            print(f"Error processing tile index: {e}")
            return 1
    
    print(f"Output directory: {args.output_dir}")
    return 0

if __name__ == "__main__":
    exit(main())