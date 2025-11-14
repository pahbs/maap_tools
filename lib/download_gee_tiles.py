#!/usr/bin/env python3
"""
Download GEE assets by tile using geedim

Usage:
    python download_gee_tiles.py --asset_path <path> --asset_type <type> --year <year> --scale <scale> --outdir <dir> --tiles_csv <csv> --gee_account <account> --gee_key_path <key_path> [--tile_list <tiles>]

Example:
    python download_gee_tiles.py --asset_path "users/username/asset" --asset_type "landcover" --year 2020 --scale 30 --outdir ./output --tiles_csv tiles_index.csv --gee_account "my-service-account@my-project.iam.gserviceaccount.com" --gee_key_path "s3://bucket/path/to/service-account-key.json" --tile_list 1,2,3
"""

import os
import sys
import argparse
import json
import tempfile
import warnings
import pandas as pd
import geopandas as gpd
from shapely.geometry import mapping
import ee
import geedim as gd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def parse_arguments():
    parser = argparse.ArgumentParser(description='Download GEE assets by tile')
    
    # GEE Authentication arguments
    parser.add_argument('--gee_account', required=True, type=str,
                       help='GEE service account email (e.g., "my-service-account@my-project.iam.gserviceaccount.com")')
    parser.add_argument('--gee_key_path', required=True, type=str,
                       help='Path to GEE service account JSON key file (local path or s3://bucket/path/to/key.json)')
    
    # Asset and processing arguments
    parser.add_argument('--asset_path', required=True, type=str,
                       help='Path to GEE asset (e.g., "users/username/asset")')
    parser.add_argument('--asset_type', required=True, type=str,
                       help='Asset type identifier for output filename')
    parser.add_argument('--year', required=True, type=int,
                       help='Year for filtering and filename')
    parser.add_argument('--scale', required=True, type=int,
                       help='Scale in meters for download')
    parser.add_argument('--outdir', required=True, type=str,
                       help='Output directory for downloaded tiles')
    parser.add_argument('--tiles_csv', required=True, type=str,
                       help='Path to CSV file with tile index (must have tile_num column and geometry)')
    parser.add_argument('--tile_list', type=str, default=None,
                       help='Comma-separated list of tile numbers to process (optional)')
    parser.add_argument('--resampling', type=str, default='near',
                       choices=['near', 'bilinear', 'cubic', 'average'],
                       help='Resampling method (default: near)')
    
    return parser.parse_args()

def download_key_from_s3(s3_path):
    """
    Download GEE key file from S3 to a temporary local file
    
    Parameters:
    -----------
    s3_path : str
        S3 path in format s3://bucket/path/to/key.json
    
    Returns:
    --------
    str
        Path to temporary local file containing the key
    """
    try:
        # Parse S3 path
        if not s3_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        
        # Remove s3:// prefix and split bucket and key
        s3_path_clean = s3_path[5:]  # Remove 's3://'
        parts = s3_path_clean.split('/', 1)
        
        if len(parts) != 2:
            raise ValueError("Invalid S3 path format. Expected: s3://bucket/path/to/file")
        
        bucket_name = parts[0]
        object_key = parts[1]
        
        print(f"Downloading GEE key from S3: {s3_path}")
        print(f"Bucket: {bucket_name}")
        print(f"Key: {object_key}")
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False)
        temp_file_path = temp_file.name
        temp_file.close()
        
        # Download file from S3
        try:
            s3_client.download_file(bucket_name, object_key, temp_file_path)
            print(f"Successfully downloaded GEE key to temporary file: {temp_file_path}")
            return temp_file_path
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"Error: S3 object not found: {s3_path}")
            elif error_code == 'NoSuchBucket':
                print(f"Error: S3 bucket not found: {bucket_name}")
            elif error_code == 'AccessDenied':
                print(f"Error: Access denied to S3 object: {s3_path}")
            else:
                print(f"Error downloading from S3: {e}")
            
            # Clean up temp file if it was created
            try:
                os.remove(temp_file_path)
            except:
                pass
            
            return None
            
    except NoCredentialsError:
        print("Error: AWS credentials not found. Configure with aws configure or set environment variables.")
        return None
    except Exception as e:
        print(f"Error processing S3 path: {e}")
        return None

def get_gee_key_path(key_path_arg):
    """
    Get the GEE key file path, downloading from S3 if necessary
    
    Parameters:
    -----------
    key_path_arg : str
        Key path argument (local path or S3 path)
    
    Returns:
    --------
    tuple
        (local_key_path, is_temporary) where is_temporary indicates if file should be cleaned up
    """
    if key_path_arg.startswith('s3://'):
        # Download from S3
        temp_key_path = download_key_from_s3(key_path_arg)
        if temp_key_path is None:
            return None, False
        return temp_key_path, True
    else:
        # Local file path
        if not os.path.exists(key_path_arg):
            print(f"Error: Local GEE key file not found: {key_path_arg}")
            return None, False
        return key_path_arg, False

def initialize_ee(service_account_email, key_file_path):
    """Initialize Earth Engine with service account credentials"""
    try:
        # Validate JSON format
        try:
            with open(key_file_path, 'r') as f:
                key_data = json.load(f)
            print(f"Successfully loaded GEE service account key")
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in key file {key_file_path}: {e}")
            return False
        except Exception as e:
            print(f"Error reading key file {key_file_path}: {e}")
            return False
        
        # Create credentials object
        credentials = ee.ServiceAccountCredentials(service_account_email, key_file_path)
        
        # Initialize Earth Engine
        ee.Initialize(credentials)
        print(f"Earth Engine initialized successfully with service account: {service_account_email}")
        
        # Test the connection by making a simple request
        try:
            test_image = ee.Image('COPERNICUS/S2_SR/20230101T000000_20230101T000000_T32UPU')
            _ = test_image.bandNames().getInfo()
            print("GEE connection test successful")
        except Exception as e:
            print(f"Warning: GEE connection test failed (this might be normal if the test image doesn't exist): {e}")
        
        return True
        
    except Exception as e:
        print(f"Error initializing Earth Engine: {e}")
        print("Please check your service account email and key file")
        return False

def load_tiles_index(tiles_csv_path, tile_list=None):
    """Load tiles index from CSV"""
    try:
        tiles_index = gpd.read_file(tiles_csv_path)
        print(f"Loaded {len(tiles_index)} tiles from {tiles_csv_path}")
        
        if tile_list:
            tile_numbers = [int(x.strip()) for x in tile_list.split(',')]
            tiles_index = tiles_index[tiles_index.tile_num.isin(tile_numbers)]
            print(f"Filtered to {len(tiles_index)} tiles based on tile_list")
            
        return tiles_index
        
    except Exception as e:
        print(f"Error loading tiles index: {e}")
        sys.exit(1)

def process_tile(focal_tile, tiles_index, asset_path, asset_type, year, scale, outdir, resampling):
    """Process a single tile"""
    
    # Generate output filename
    output_filename = f'{outdir}/{asset_type}_{year}_{scale}m_{focal_tile:07}.tif'
    
    # Check if file already exists
    if os.path.exists(output_filename):
        print(f"Skipping tile {focal_tile} - file already exists: {output_filename}")
        return True
    
    print(f"Processing tile {focal_tile}...")
    
    try:
        # Get tile geometry
        tiles_index_focal = tiles_index[tiles_index.tile_num.isin([focal_tile])]
        if len(tiles_index_focal) == 0:
            print(f"Warning: Tile {focal_tile} not found in tiles index")
            return False
            
        tile_geom_4326 = tiles_index_focal.to_crs(4326).geometry.iloc[0]
        ee_geom = ee.Geometry(mapping(tile_geom_4326))
        
        # Check what type of asset it is
        try:
            asset_info = ee.data.getAsset(asset_path)
            asset_type_gee = asset_info['type']
            
            if asset_type_gee == 'IMAGE_COLLECTION':
                gee_image_collection = ee.ImageCollection(asset_path) \
                    .filterBounds(ee_geom) \
                    .filterDate(f'{year}-01-01', f'{year+1}-01-01')
                gee_image = gee_image_collection.toBands().clip(ee_geom)
            elif asset_type_gee == 'IMAGE':
                gee_image = ee.Image(asset_path).clip(ee_geom)
            else:
                raise ValueError(f"Unsupported asset type: {asset_type_gee}")
                
        except Exception as e:
            print(f"Error accessing asset for tile {focal_tile}: {e}")
            return False
        
        try:
            # Start download using MaskedImage (suppressing deprecation warning)
            print(f"Downloading tile {focal_tile} to {output_filename}")
            
            # Suppress the deprecation warning for MaskedImage
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning, module="geedim.mask")
                
                im = gd.mask.MaskedImage(gee_image)
                im.download(output_filename, region=ee_geom, 
                          crs=f'EPSG:4326', 
                          scale=scale,
                          resampling=resampling)
            
            # Verify download worked
            if os.path.exists(output_filename):
                print(f"Successfully downloaded tile {focal_tile}")
                return True
            else:
                print(f"Warning: Download appeared to succeed but file not found: {output_filename}")
                return False
            
        except Exception as e:
            print(f"Error downloading tile {focal_tile}: {e}")
            # If partial download exists, remove it
            if os.path.exists(output_filename):
                try:
                    os.remove(output_filename)
                    print(f"Removed partial download of tile {focal_tile}")
                except:
                    print(f"Warning: Could not remove partial download of tile {focal_tile}")
            return False
            
    except Exception as e:
        print(f"Error processing tile {focal_tile}: {e}")
        return False

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Get GEE key file path (download from S3 if necessary)
    print("Getting GEE key file...")
    local_key_path, is_temporary = get_gee_key_path(args.gee_key_path)
    
    if local_key_path is None:
        sys.exit(1)
    
    try:
        # Initialize Earth Engine with service account
        print("Initializing Google Earth Engine...")
        if not initialize_ee(args.gee_account, local_key_path):
            sys.exit(1)
        
        # Create output directory if it doesn't exist
        os.makedirs(args.outdir, exist_ok=True)
        
        # Load tiles index
        tiles_index = load_tiles_index(args.tiles_csv, args.tile_list)
        
        # Get list of tiles to process
        if args.tile_list:
            tile_numbers = [int(x.strip()) for x in args.tile_list.split(',')]
        else:
            tile_numbers = tiles_index.tile_num.tolist()
        
        print(f"Processing {len(tile_numbers)} tiles...")
        print(f"Asset path: {args.asset_path}")
        print(f"Resampling: {args.resampling}")
        
        # Process tiles
        successful = 0
        failed = 0
        
        for tile_num in tile_numbers:
            success = process_tile(
                focal_tile=tile_num,
                tiles_index=tiles_index,
                asset_path=args.asset_path,
                asset_type=args.asset_type,
                year=args.year,
                scale=args.scale,
                outdir=args.outdir,
                resampling=args.resampling
            )
            
            if success:
                successful += 1
            else:
                failed += 1
        
        print(f"\nProcessing complete:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Total: {len(tile_numbers)}")
        
    finally:
        # Clean up temporary key file if it was downloaded from S3
        if is_temporary and local_key_path and os.path.exists(local_key_path):
            try:
                os.remove(local_key_path)
                print(f"Cleaned up temporary key file: {local_key_path}")
            except Exception as e:
                print(f"Warning: Could not remove temporary key file {local_key_path}: {e}")

if __name__ == "__main__":
    main()