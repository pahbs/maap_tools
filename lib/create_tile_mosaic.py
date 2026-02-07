#!/usr/bin/env python3
"""
Create a raster mosaic from tiles listed in a CSV index file

Usage:
    python create_tile_mosaic.py --tile_index_csv tiles.csv --output_path mosaic.tif [options]

Example:
    python create_tile_mosaic.py --tile_index_csv tiles.csv --output_path mosaic.tif --tile_list 1,2,3,5 --target_crs EPSG:4326 --compress ZSTD --target_resolution 30
"""

import argparse
import pandas as pd
import rasterio
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.enums import Resampling as ResamplingEnum
from pathlib import Path
import numpy as np
import sys
import os
import warnings
import tempfile

# Suppress rasterio warnings for cleaner output
warnings.filterwarnings('ignore', category=rasterio.errors.NotGeoreferencedWarning)

def setup_s3fs():
    """Setup S3 filesystem for reading remote files."""
    try:
        import s3fs
        return s3fs.S3FileSystem(anon=True)
    except ImportError:
        print("Warning: s3fs not available. Install with: pip install s3fs")
        return None

def get_numpy_dtype_from_string(dtype_str):
    """Convert string data type to numpy dtype."""
    dtype_mapping = {
        'byte': np.uint8,
        'uint8': np.uint8,
        'int8': np.int8,
        'uint16': np.uint16,
        'int16': np.int16,
        'uint32': np.uint32,
        'int32': np.int32,
        'float32': np.float32,
        'float64': np.float64
    }
    
    return dtype_mapping.get(dtype_str.lower(), np.uint8)

def get_rasterio_dtype_from_string(dtype_str):
    """Convert string data type to rasterio dtype string."""
    dtype_mapping = {
        'byte': 'uint8',
        'uint8': 'uint8',
        'int8': 'int8',
        'uint16': 'uint16', 
        'int16': 'int16',
        'uint32': 'uint32',
        'int32': 'int32',
        'float32': 'float32',
        'float64': 'float64'
    }
    
    return dtype_mapping.get(dtype_str.lower(), 'uint8')

def get_compatible_nodata_value(original_nodata, output_dtype):
    """Get a nodata value compatible with the output data type."""
    np_dtype = get_numpy_dtype_from_string(output_dtype)
    
    # Get the valid range for the data type
    if np_dtype == np.uint8:
        valid_range = (0, 255)
        default_nodata = 0
    elif np_dtype == np.int8:
        valid_range = (-128, 127)
        default_nodata = -128
    elif np_dtype == np.uint16:
        valid_range = (0, 65535)
        default_nodata = 0
    elif np_dtype == np.int16:
        valid_range = (-32768, 32767)
        default_nodata = -32768
    elif np_dtype == np.uint32:
        valid_range = (0, 4294967295)
        default_nodata = 0
    elif np_dtype == np.int32:
        valid_range = (-2147483648, 2147483647)
        default_nodata = -2147483648
    else:
        # For float types, original nodata is usually fine
        return original_nodata
    
    # Check if original nodata is compatible
    if original_nodata is not None:
        if valid_range[0] <= original_nodata <= valid_range[1]:
            return original_nodata
    
    # Return a safe default
    return default_nodata

def extract_band_to_temp_file(src_path, band, output_dtype, compatible_nodata):
    """Extract a specific band to a temporary file."""
    temp_fd, temp_path = tempfile.mkstemp(suffix='.tif')
    os.close(temp_fd)
    
    try:
        with rasterio.open(src_path) as src:
            if band > src.count:
                raise ValueError(f"Band {band} not available (file has {src.count} bands)")
            
            # Read the specific band
            band_data = src.read(band)
            
            # Update profile for single band output
            profile = src.profile.copy()
            profile.update({
                'count': 1,
                'dtype': get_rasterio_dtype_from_string(output_dtype),
                'nodata': compatible_nodata
            })
            
            # Convert data type and handle nodata
            np_dtype = get_numpy_dtype_from_string(output_dtype)
            
            # Handle original nodata values
            if src.nodata is not None:
                band_data = np.where(band_data == src.nodata, compatible_nodata, band_data)
            
            # Convert to target data type
            band_data = band_data.astype(np_dtype)
            
            # Write to temporary file
            with rasterio.open(temp_path, 'w', **profile) as dst:
                dst.write(band_data, 1)
        
        return temp_path
        
    except Exception as e:
        # Clean up temp file if creation failed
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise e

def create_tile_mosaic(tile_index_csv, output_path, tile_column='tile_num', 
                      path_column='file_path', tile_list=None, 
                      target_crs=None, target_resolution=None, resampling_method='nearest',
                      compress='LZW', tiled=True, blocksize=512,
                      nodata_value=None, enable_s3=False, verbose=False,
                      base_name=None, band=None, output_dtype='uint8'):
    """
    Create a mosaic from tiles listed in a CSV index file
    
    Parameters:
    -----------
    tile_index_csv : str
        Path to CSV file containing tile information
    output_path : str
        Path for output mosaic file (can be directory or full path with .tif extension)
    tile_column : str
        Column name containing tile identifiers
    path_column : str  
        Column name containing file paths to rasters
    tile_list : list or None
        List of tile numbers to include (if None, includes all tiles)
    target_crs : str or None
        Target CRS for output (e.g., "EPSG:4326")
    target_resolution : float or None
        Target resolution in units of target CRS
    resampling_method : str
        Resampling method ('nearest', 'bilinear', 'cubic', 'average', 'mode')
    compress : str
        Compression method for output
    tiled : bool
        Whether to create tiled output
    blocksize : int
        Block size for tiled output  
    nodata_value : float or None
        NoData value for output
    enable_s3 : bool
        Enable S3 filesystem support
    verbose : bool
        Enable verbose output
    base_name : str or None
        Base name for output file
    band : int or None
        Specific band to extract from input files (1-indexed)
    output_dtype : str
        Output data type ('uint8', 'int16', 'float32', etc.)
    
    Returns:
    --------
    str
        Path to created mosaic file
    """
    
    def vprint(*args, **kwargs):
        """Verbose print function"""
        if verbose:
            print(*args, **kwargs)
    
    temp_files = []  # Track temporary files for cleanup
    
    try:
        # Setup S3 filesystem if needed
        s3fs_obj = None
        if enable_s3:
            s3fs_obj = setup_s3fs()
            if s3fs_obj:
                print("S3 filesystem initialized with anonymous access")
        
        # Read tile index CSV
        print(f"Reading tile index from: {tile_index_csv}")
        if not Path(tile_index_csv).exists():
            raise FileNotFoundError(f"Tile index CSV not found: {tile_index_csv}")
        
        tile_df = pd.read_csv(tile_index_csv)
        vprint(f"Loaded CSV with {len(tile_df)} rows and columns: {list(tile_df.columns)}")
        
        # Validate required columns
        if tile_column not in tile_df.columns:
            raise ValueError(f"Column '{tile_column}' not found in CSV. Available columns: {list(tile_df.columns)}")
        if path_column not in tile_df.columns:
            raise ValueError(f"Column '{path_column}' not found in CSV. Available columns: {list(tile_df.columns)}")
        
        # Filter tiles if tile_list is provided
        if tile_list is not None:
            if isinstance(tile_list, str):
                if tile_list.lower() == 'all':
                    tiles_to_process = tile_df
                else:
                    try:
                        tile_numbers = [int(x.strip()) for x in tile_list.split(',')]
                        tiles_to_process = tile_df[tile_df[tile_column].isin(tile_numbers)]
                    except ValueError:
                        raise ValueError("Invalid tile_list format. Use comma-separated integers or 'all'")
            elif isinstance(tile_list, list):
                tiles_to_process = tile_df[tile_df[tile_column].isin(tile_list)]
            else:
                raise ValueError("tile_list must be a string, list, or None")
        else:
            tiles_to_process = tile_df
        
        if len(tiles_to_process) == 0:
            raise ValueError("No tiles to process after filtering")
        
        print(f"Processing {len(tiles_to_process)} tiles")
        vprint(f"Tiles to process: {sorted(tiles_to_process[tile_column].tolist())}")
        
        # Collect valid file paths
        valid_files = []
        file_paths = tiles_to_process[path_column].tolist()
        
        for i, file_path in enumerate(file_paths):
            if pd.isna(file_path):
                vprint(f"Skipping tile {i+1}: path is NaN")
                continue
                
            file_path = str(file_path).strip()
            if not file_path:
                vprint(f"Skipping tile {i+1}: empty path")
                continue
            
            # Handle S3 paths
            if file_path.startswith('s3://') and s3fs_obj:
                try:
                    if s3fs_obj.exists(file_path):
                        valid_files.append(file_path)
                        vprint(f"Found S3 file: {file_path}")
                    else:
                        print(f"S3 file not found: {file_path}")
                except Exception as e:
                    print(f"Error checking S3 file {file_path}: {e}")
            else:
                # Local file
                if os.path.exists(file_path):
                    valid_files.append(file_path)
                    vprint(f"Found local file: {file_path}")
                else:
                    print(f"Local file not found: {file_path}")
        
        if not valid_files:
            raise FileNotFoundError("No valid raster files found")
        
        print(f"Found {len(valid_files)} valid raster files")
        
        # Get compatible nodata value
        # First, check what nodata values exist in the source files
        source_nodata_values = set()
        with rasterio.open(valid_files[0]) as src:
            if src.nodata is not None:
                source_nodata_values.add(src.nodata)
        
        # Determine compatible nodata value
        if nodata_value is not None:
            compatible_nodata = nodata_value
        else:
            original_nodata = list(source_nodata_values)[0] if source_nodata_values else None
            compatible_nodata = get_compatible_nodata_value(original_nodata, output_dtype)
        
        print(f"Using nodata value: {compatible_nodata} (compatible with {output_dtype})")
        
        # Process files - extract band and convert data type if needed
        files_to_mosaic = []
        
        if band is not None:
            print(f"Extracting band {band} and converting to {output_dtype}...")
            for file_path in valid_files:
                try:
                    temp_file = extract_band_to_temp_file(file_path, band, output_dtype, compatible_nodata)
                    temp_files.append(temp_file)
                    files_to_mosaic.append(temp_file)
                    vprint(f"Extracted band {band} from {file_path}")
                except Exception as e:
                    print(f"Error extracting band from {file_path}: {e}")
                    continue
        else:
            files_to_mosaic = valid_files
        
        if not files_to_mosaic:
            raise ValueError("No files available for mosaicking after band extraction")
        
        # Open datasets for mosaicking
        datasets = []
        reference_dataset = None
        
        for file_path in files_to_mosaic:
            try:
                dataset = rasterio.open(file_path)
                datasets.append(dataset)
                
                if reference_dataset is None:
                    reference_dataset = dataset
                    vprint(f"Reference dataset: {file_path}")
                    vprint(f"  Shape: {dataset.shape}")
                    vprint(f"  CRS: {dataset.crs}")
                    vprint(f"  Data type: {dataset.dtypes[0]}")
                    vprint(f"  NoData: {dataset.nodata}")
                    vprint(f"  Bands: {dataset.count}")
                
            except Exception as e:
                print(f"Error opening {file_path}: {e}")
                continue
        
        if not datasets:
            raise ValueError("No datasets could be opened successfully")
        
        print(f"Opened {len(datasets)} datasets for mosaicking")
        
        # Determine output parameters
        if target_crs is None:
            target_crs = reference_dataset.crs
            print(f"Using CRS from reference dataset: {target_crs}")
        
        if target_resolution is None:
            target_resolution = abs(reference_dataset.transform[0])
            print(f"Using resolution from reference dataset: {target_resolution}")
        
        # Create output filename if output_path is a directory
        if os.path.isdir(output_path) or not output_path.endswith('.tif'):
            if not base_name:
                base_name = "mosaic"
            
            # Build filename with metadata
            filename_parts = [base_name]
            if band is not None:
                filename_parts.append(f"band{band}")
            #filename_parts.append(output_dtype.lower())
            filename_parts.append(f"{len(datasets)}tiles")
            filename_parts.append(f"{target_resolution:.6f}".replace('.','p'))
            
            filename = "_".join(filename_parts) + ".tif"
            final_output_path = os.path.join(output_path, filename)
        else:
            final_output_path = output_path
        
        # Create output directory if needed
        os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
        
        print(f"Output file: {final_output_path}")
        
        # Create mosaic
        print("Creating mosaic...")
        vprint(f"Resampling method: {resampling_method}")
        vprint(f"Target CRS: {target_crs}")
        vprint(f"Target resolution: {target_resolution}")
        
        # Get resampling enum
        resampling_enum = getattr(ResamplingEnum, resampling_method)
        
        # Convert data type strings
        np_dtype = get_numpy_dtype_from_string(output_dtype)
        rio_dtype = get_rasterio_dtype_from_string(output_dtype)
        
        # Create mosaic with proper data type and nodata
        mosaic, out_transform = merge(
            datasets,
            resampling=resampling_enum,
            nodata=compatible_nodata,
            dtype=np_dtype,
            res=target_resolution if target_resolution != abs(reference_dataset.transform[0]) else None
        )
        
        vprint(f"Mosaic array shape: {mosaic.shape}")
        vprint(f"Mosaic array dtype: {mosaic.dtype}")
        
        # Prepare output metadata
        out_meta = reference_dataset.meta.copy()
        
        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2], 
            "transform": out_transform,
            "crs": target_crs,
            "nodata": compatible_nodata,
            "dtype": rio_dtype,
            "count": mosaic.shape[0]
        })
        
        # Add compression if specified
        if compress.upper() != 'NONE':
            out_meta["compress"] = compress
        
        # Add tiling if specified
        if tiled:
            out_meta.update({
                "tiled": True,
                "blockxsize": blocksize,
                "blockysize": blocksize
            })
        
        # Write output
        print(f"Writing mosaic to: {final_output_path}")
        with rasterio.open(final_output_path, 'w', **out_meta) as dest:
            dest.write(mosaic)
        
        # Close datasets
        for dataset in datasets:
            dataset.close()
        
        # Verify output
        with rasterio.open(final_output_path) as result:
            print(f"Mosaic created successfully:")
            print(f"  Shape: {result.shape}")
            print(f"  Data type: {result.dtypes[0]}")
            print(f"  CRS: {result.crs}")
            print(f"  NoData: {result.nodata}")
            print(f"  Resolution: {abs(result.transform[0])}")
            if band:
                print(f"  Band extracted: {band}")
            
            # Check for expected integer values if classification data
            if output_dtype in ['uint8', 'int8', 'uint16', 'int16']:
                sample_data = result.read(1, window=rasterio.windows.Window(0, 0, min(1000, result.width), min(1000, result.height)))
                unique_vals = np.unique(sample_data[sample_data != compatible_nodata])
                if len(unique_vals) <= 20:  # Likely classification data
                    print(f"  Sample values: {unique_vals[:10]}{'...' if len(unique_vals) > 10 else ''}")
        
        return final_output_path
        
    except Exception as e:
        print(f"Error creating mosaic: {e}")
        raise
    
    finally:
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
                    vprint(f"Cleaned up temp file: {temp_file}")
            except Exception as e:
                print(f"Warning: Could not clean up temp file {temp_file}: {e}")

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Create a raster mosaic from tiles listed in a CSV index file',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required arguments
    parser.add_argument('--tile_index_csv', required=True, type=str,
                       help='Path to CSV file containing tile information')
    parser.add_argument('--output_path', required=True, type=str,
                       help='Path for output mosaic file (can be directory or full path with .tif extension)')
    
    # New arguments for band and data type control
    parser.add_argument('-o', '--output', type=str, dest='output_path',
                       help='Alias for --output_path')
    parser.add_argument('--band', type=int, default=None,
                       help='Specific band to extract from input files (1-indexed)')
    parser.add_argument('--output_dtype', type=str, default='uint8',
                       choices=['byte', 'uint8', 'int8', 'uint16', 'int16', 'uint32', 'int32', 'float32', 'float64'],
                       help='Output data type')
    
    # CSV column configuration
    parser.add_argument('--tile_column', type=str, default='tile_num',
                       help='Column name containing tile identifiers')
    parser.add_argument('--path_column', type=str, default='file_path',
                       help='Column name containing file paths to rasters')
    
    # Tile selection
    parser.add_argument('--tile_list', type=str, default=None,
                       help='Comma-separated list of tile numbers to include (e.g., "1,2,3,5") or "all"')
    
    # CRS and resampling
    parser.add_argument('--target_crs', type=str, default=None,
                       help='Target CRS for output (e.g., "EPSG:4326"). If not specified, uses CRS from first tile')
    parser.add_argument('--target_resolution', type=float, default=None,
                       help='Target resolution in units of target CRS (e.g., 30 for 30m pixels)')
    parser.add_argument('--resampling_method', type=str, default='nearest',
                       choices=['nearest', 'bilinear', 'cubic', 'average', 'mode'],
                       help='Resampling method for merging tiles')
    
    # Output format options
    parser.add_argument('--compress', type=str, default='LZW',
                       choices=['LZW', 'DEFLATE', 'ZSTD', 'JPEG', 'WEBP', 'NONE'],
                       help='Compression method for output')
    parser.add_argument('--tiled', action='store_true', default=True,
                       help='Create tiled output (enabled by default)')
    parser.add_argument('--no_tiled', dest='tiled', action='store_false',
                       help='Disable tiled output')
    parser.add_argument('--blocksize', type=int, default=512,
                       help='Block size for tiled output')
    parser.add_argument('--nodata_value', type=float, default=None,
                       help='NoData value for output. If not specified, uses value from first tile')
    
    # S3 and file handling
    parser.add_argument('--enable_s3', action='store_true',
                       help='Enable S3 support for reading remote files directly (requires s3fs)')
    
    # Filename generation
    parser.add_argument('--base_name', type=str, default=None,
                       help='Base name for auto-generated output filename when output_path is a directory')
    
    # Verbose output
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose output for debugging')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Parse tile list if provided
    tile_list = None
    if args.tile_list:
        if args.tile_list.lower() == 'all':
            tile_list = None
        else:
            try:
                tile_list = [int(x.strip()) for x in args.tile_list.split(',')]
            except ValueError:
                print("Error: Invalid tile_list format. Use comma-separated integers (e.g., '1,2,3,5') or 'all'")
                sys.exit(1)
    
    # Check if S3 support is needed but s3fs not available
    if args.enable_s3:
        try:
            import s3fs
        except ImportError:
            print("Error: s3fs is required for S3 support. Install with: pip install s3fs")
            sys.exit(1)
    
    # Print configuration
    print("Mosaic Configuration:")
    print(f"  Input CSV: {args.tile_index_csv}")
    print(f"  Output path: {args.output_path}")
    print(f"  Tile column: {args.tile_column}")
    print(f"  Path column: {args.path_column}")
    if args.band:
        print(f"  Band: {args.band}")
    print(f"  Output data type: {args.output_dtype}")
    if tile_list:
        print(f"  Tile list: {tile_list} ({len(tile_list)} tiles)")
    else:
        print(f"  Tile list: All tiles")
    print(f"  Target CRS: {args.target_crs or 'Auto (from first tile)'}")
    print(f"  Target resolution: {args.target_resolution or 'Auto (from first tile)'}")
    print(f"  Resampling: {args.resampling_method}")
    print(f"  Compression: {args.compress}")
    print(f"  Tiled: {args.tiled}")
    if args.tiled:
        print(f"  Block size: {args.blocksize}x{args.blocksize}")
    print(f"  S3 support: {args.enable_s3}")
    print()
    
    try:
        output_file = create_tile_mosaic(
            tile_index_csv=args.tile_index_csv,
            output_path=args.output_path,
            tile_column=args.tile_column,
            path_column=args.path_column,
            tile_list=tile_list,
            target_crs=args.target_crs,
            target_resolution=args.target_resolution,
            resampling_method=args.resampling_method,
            compress=args.compress,
            tiled=args.tiled,
            blocksize=args.blocksize,
            nodata_value=args.nodata_value,
            enable_s3=args.enable_s3,
            verbose=args.verbose,
            base_name=args.base_name,
            band=args.band,
            output_dtype=args.output_dtype
        )
        
        print(f"\n✓ Mosaic creation completed successfully!")
        print(f"Output: {output_file}")
        
    except Exception as e:
        print(f"Error creating mosaic: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()