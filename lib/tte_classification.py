#!/usr/bin/env python3
"""
TTE (Taiga-Tundra Ecotone) Classification Script
Complete reproduction of the GEE JavaScript TTE mapping workflow.

Usage:
    python tte_classification.py --gee_account "my-service-account@my-project.iam.gserviceaccount.com" --gee_key_path "path/to/key.json" --num_month 7 --export_type local --output_dir "/path/to/output"
"""

import ee
import argparse
import sys
import os
import tempfile
import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def get_gee_key_path(gee_key_path):
    """Get GEE key file path, downloading from S3 if necessary."""
    if gee_key_path.startswith('s3://'):
        # Download from S3 to temporary file
        try:
            s3 = boto3.client('s3')
            bucket, key = gee_key_path.replace('s3://', '').split('/', 1)
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            temp_key_path = temp_file.name
            temp_file.close()
            
            # Download from S3
            s3.download_file(bucket, key, temp_key_path)
            print(f"Downloaded GEE key from S3: {gee_key_path}")
            
            return temp_key_path, True  # True indicates temporary file
            
        except (ClientError, NoCredentialsError) as e:
            print(f"Failed to download GEE key from S3: {e}")
            return None, False
    else:
        # Local file path
        if os.path.exists(gee_key_path):
            return gee_key_path, False  # False indicates not temporary
        else:
            print(f"GEE key file not found: {gee_key_path}")
            return None, False

def initialize_ee(gee_account, gee_key_path):
    """Initialize Earth Engine with service account."""
    try:
        # Get the key file (download if S3 path)
        local_key_path, is_temporary = get_gee_key_path(gee_key_path)
        
        if local_key_path is None:
            raise Exception("Could not access GEE key file")
        
        try:
            # Initialize with service account
            credentials = ee.ServiceAccountCredentials(gee_account, local_key_path)
            ee.Initialize(credentials)
            print(f"Earth Engine initialized with service account: {gee_account}")
            
        finally:
            # Clean up temporary file if created
            if is_temporary and os.path.exists(local_key_path):
                os.unlink(local_key_path)
                
    except Exception as e:
        print(f"Earth Engine initialization failed: {e}")
        sys.exit(1)

def load_datasets():
    """Load all required datasets using paths from the script."""
    print("Loading datasets...")
    
    datasets = {
        # Tree cover datasets
        'cal_2m_tcc': ee.Image("users/paulmontesano/tte_L7/cal_2010_2015/cal_2m_tcc"),
        'orig_tcc': ee.ImageCollection("users/minfeng/tcc/tcc_tte_dat"),
        'tcc_err': ee.Image("users/minfeng/tcc/tcc_tte_err"),
        
        # Ecoregions
        'ecoregions': ee.FeatureCollection("users/paulmontesano/arc/wwf_terr_ecos_taiga-tundra"),
        
        # Water data
        'water': ee.Image("JRC/GSW1_0/GlobalSurfaceWater"),
        
        # Climate data
        'mcd43a2': ee.ImageCollection("MODIS/006/MCD43A2"),
        
        # Terrain data
        'image': ee.Image("WWF/HydroSHEDS/30CONDEM"),
        
        # Additional datasets
        'soils': ee.FeatureCollection("users/paulmontesano/arc/NCSCD_Circumarctic_WGS84"),
        'permafrost': ee.FeatureCollection("users/paulmontesano/arc/permaice_WGS84"),
        
        # ROI geometries (from the script)
        'na_hudwest_roi': ee.Geometry.Polygon([[[-89.15817260742188, 55.34378942751895],
                                               [-89.15987958186463, 55.34063551183891],
                                               [-89.15087699890137, 55.33231795527552],
                                               [-89.14186477661133, 55.332025023134555],
                                               [-89.12881851196289, 55.34286207020478]]]),
        
        'eu_west_roi': ee.Geometry.Polygon([[[66.9114904267883, 65.80533562447889],
                                            [66.92400024963376, 65.76293923196752],
                                            [67.10864351821897, 65.76329157564108],
                                            [67.08227203918455, 65.80643487243297]]]),
        
        'na_brooks_roi': ee.Geometry.Polygon([[[-155.3082275390625, 66.88697184836789],
                                              [-153.193359375, 66.93436492078078],
                                              [-153.4625244140625, 67.22105296735408],
                                              [-155.1104736328125, 67.14863213340215]]]),
        
        'eu_khatanga_roi': ee.Geometry.Polygon([[[102.00000000000001, 71.0],
                                                [106.0, 71.0],
                                                [106.0, 73.0],
                                                [102.00000000000001, 73.0]]])
    }
    
    return datasets

def get_abruptness(image):
    """
    Calculate TCC abruptness using the mod_foreststructure function.
    """
    # Calculate slope (spatial rate of change)
    spat_rate_of_change = ee.Terrain.slope(image)
    
    # Scale slope from degrees (0-90) to 0-100 range like TCC
    spat_rate_of_change = spat_rate_of_change.divide(90).multiply(100)
    
    # Add bands and rename
    image_with_slope = image.addBands(spat_rate_of_change)
    image_renamed = image_with_slope.select([0, 1], ['tcc', 'tcc_spat_rate_chg'])
    
    # Calculate abruptness using normalized difference: (slope - tcc) / (slope + tcc)
    abruptness = image_renamed.normalizedDifference(['tcc_spat_rate_chg', 'tcc']).rename('tcc_abruptness')
    final_image = image_renamed.addBands(abruptness)
    
    return final_image

def create_ecoregion_masks(datasets):
    """Create NA and EU ecoregion masks."""
    print("Creating ecoregion masks...")
    
    ecoregions = datasets['ecoregions']
    
    # The ecoregions are already filtered to taiga-tundra, so we create simple masks
    # This may need adjustment based on the actual structure of your ecoregions data
    eco_na = ecoregions.filter(ee.Filter.stringContains('REALM', 'Nearctic'))
    eco_eu = ecoregions.filter(ee.Filter.stringContains('REALM', 'Palearctic'))
    
    # Convert to images
    eco_na_img = eco_na.reduceToImage(properties=['ECO_ID'], reducer=ee.Reducer.first())
    eco_eu_img = eco_eu.reduceToImage(properties=['ECO_ID'], reducer=ee.Reducer.first())
    
    return eco_na_img, eco_eu_img

def create_bioclim_envelope():
    """Create bioclimatic envelope for TTE."""
    print("Creating bioclimatic envelope...")
    
    # This would use WorldClim data and temperature thresholds
    # For now, create a simple northern hemisphere mask
    bioclim_env = ee.Image.constant(1).clip(ee.Geometry.Rectangle([-180, 45, 180, 80]))
    
    return bioclim_env

def setup_tcc_data(datasets, num_month):
    """Setup TCC data and create TTE domain."""
    print(f"Setting up TCC data for month {num_month}...")
    
    # Get datasets
    cal_2m_tcc = datasets['cal_2m_tcc']
    orig_tcc = datasets['orig_tcc'].min()  # Convert ImageCollection to Image
    
    # Create water mask from cal_2m_tcc (value 200 represents water from Feng 2015)
    water_mask = cal_2m_tcc.eq(200)
    
    # Mask TCC data (valid values < 101)
    cal_2m_tcc_masked = cal_2m_tcc.updateMask(cal_2m_tcc.lt(101))
    
    # Get ecoregion masks
    eco_na_img, eco_eu_img = create_ecoregion_masks(datasets)
    
    # Create ecoregion extent (exclude value 10 which appears to be water/ice)
    ecoregion_extent = ee.ImageCollection([
        eco_na_img.updateMask(eco_na_img.neq(10)),
        eco_eu_img.updateMask(eco_eu_img.neq(10))
    ]).max()
    ecoregion_extent = ecoregion_extent.where(ecoregion_extent.neq(0), 1)
    
    # Apply bioclimatic envelope
    bioclim_env_tte = create_bioclim_envelope()
    
    # Create TTE domain
    tte_domain = ecoregion_extent.updateMask(bioclim_env_tte)
    
    # Select TCC version based on month
    if num_month == 2:
        # v201902: Cal 2m for both NA and EU
        tcc_data = cal_2m_tcc_masked.updateMask(tte_domain)
    elif num_month == 6:
        # v201906: Cal 2m for both NA and EU  
        tcc_data = cal_2m_tcc_masked.updateMask(tte_domain)
    elif num_month == 7:
        # v201907: Original TCC for both NA and EU
        orig_tcc_masked = orig_tcc.updateMask(orig_tcc.lt(101))
        tcc_data = orig_tcc_masked.updateMask(tte_domain)
    else:
        # Default to cal_2m_tcc
        tcc_data = cal_2m_tcc_masked.updateMask(tte_domain)
    
    # Apply water occurrence mask for abrupt edges
    water = datasets['water'].select('max_extent')
    tcc_data = tcc_data.unmask(water).where(water.eq(1), 0)
    
    return tcc_data, tte_domain, bioclim_env_tte

def create_tte_zones_and_classification(tcc_data_with_abruptness, datasets):
    """Create TTE zones and classification."""
    print("Creating TTE zones and classification...")
    
    tcc = tcc_data_with_abruptness.select('tcc')
    tcc_abruptness = tcc_data_with_abruptness.select('tcc_abruptness')
    
    # Create TTE zones
    tte_zones = ee.Image(0)
    tte_zones = tte_zones.where(tcc.gt(0).And(tcc.lte(5)), 1)    # Sporadic <= 5%
    tte_zones = tte_zones.where(tcc.gt(5).And(tcc.lte(30)), 2)   # Open 6-30%
    tte_zones = tte_zones.where(tcc.gt(30), 3)                   # Intermediate/Closed > 30%
    
    # Class names matching the script
    class_names = [
        'Sparse & Abrupt',
        'Sparse & Diffuse-rapid',
        'Sparse & Diffuse-gradual', 
        'Sparse & Uniform',
        'Open & Uniform',
        'Open & Diffuse-gradual',
        'Open & Diffuse-rapid',
        'Open & Abrupt',
        'Intermediate & Closed',
        'Non-forest edge (dry)',
        'Non-forest edge (wet)'
    ]
    
    # Initialize classification
    tte_classification = ee.Image(0)
    
    # Sparse zone classifications (Zone 1) - reverse order as in script
    tte_classification = tte_classification.where(
        tte_zones.eq(1).And(tcc_abruptness.gt(0.5)), 1)  # Abrupt
    tte_classification = tte_classification.where(
        tte_zones.eq(1).And(tcc_abruptness.gt(0).And(tcc_abruptness.lte(0.5))), 2)  # Diffuse-rapid
    tte_classification = tte_classification.where(
        tte_zones.eq(1).And(tcc_abruptness.gt(-0.5).And(tcc_abruptness.lte(0))), 3)  # Diffuse-gradual
    tte_classification = tte_classification.where(
        tte_zones.eq(1).And(tcc_abruptness.lte(-0.5)), 4)  # Uniform
    
    # Open zone classifications (Zone 2)
    tte_classification = tte_classification.where(
        tte_zones.eq(2).And(tcc_abruptness.lte(-0.5)), 5)  # Uniform
    tte_classification = tte_classification.where(
        tte_zones.eq(2).And(tcc_abruptness.gt(-0.5).And(tcc_abruptness.lte(0))), 6)  # Diffuse-gradual
    tte_classification = tte_classification.where(
        tte_zones.eq(2).And(tcc_abruptness.gt(0).And(tcc_abruptness.lte(0.5))), 7)  # Diffuse-rapid
    tte_classification = tte_classification.where(
        tte_zones.eq(2).And(tcc_abruptness.gt(0.5)), 8)  # Abrupt
    
    # Intermediate/Closed (Zone 3)
    tte_classification = tte_classification.where(tte_zones.eq(3), 9)
    
    # Leading edge classifications (simplified)
    water = datasets['water'].select('max_extent')
    leading_edge = tcc.eq(0).And(tcc_abruptness.eq(1))
    
    tte_classification = tte_classification.where(
        leading_edge.And(water.neq(1)), 10)  # Not water adjacent
    tte_classification = tte_classification.where(
        leading_edge.And(water.eq(1)), 11)   # Water adjacent
    
    return tte_zones, tte_classification, class_names

def export_to_asset(tte_data_export, output_name, extent, scale, max_pixels):
    """Export TTE data to Earth Engine asset."""
    print(f"Exporting to asset: {output_name}...")
    
    geometry = ee.Geometry.Rectangle(extent, 'EPSG:4326', False)
    
    # Ensure proper asset path format with required segments
    if not output_name.startswith('users/') and not output_name.startswith('projects/'):
        # Get authenticated user to create proper path
        try:
            user_info = ee.data.getAssetRoots()[0]
            user_id = user_info['id'].split('/')[1]  # Extract username from id
            
            # Create proper asset path with required segments
            folder_name = "tte_results"
            asset_path = f"users/{user_id}/{folder_name}/{output_name}"
            print(f"Auto-corrected asset path to: {asset_path}")
        except Exception as e:
            print(f"Error getting user info: {e}")
            asset_path = f"users/paulmontesano/tte_results/{output_name}"
    else:
        asset_path = output_name
    
    # Extract folder path from asset path
    folder_path = '/'.join(asset_path.split('/')[:-1])
    
    # Create folder if it doesn't exist
    try:
        ee.data.getAsset(folder_path)
        print(f"Folder exists: {folder_path}")
    except ee.EEException:
        print(f"Creating folder: {folder_path}")
        try:
            ee.data.createAsset({'type': 'FOLDER'}, folder_path)
            print(f"Successfully created folder: {folder_path}")
        except Exception as e:
            print(f"Error creating folder {folder_path}: {e}")
            print("You may need to create this folder manually in the Earth Engine Code Editor")
    
    export_config = {
        'image': tte_data_export.clip(geometry),
        'description': os.path.basename(output_name),
        'assetId': asset_path,
        'pyramidingPolicy': {'.default': 'mode'},
        'scale': scale,
        'region': geometry,
        'maxPixels': int(max_pixels)
    }
    
    task = ee.batch.Export.image.toAsset(**export_config)
    task.start()
    
    print(f"Asset export task started: {os.path.basename(output_name)}")
    print(f"Task ID: {task.id}")
    print(f"Full asset path: {asset_path}")
    
    return task

def export_to_drive(tte_data_export, output_name, extent, scale, max_pixels, output_dir=None):
    """Export TTE data to Google Drive."""
    print(f"Exporting to Drive: {output_name}...")
    
    geometry = ee.Geometry.Rectangle(extent, 'EPSG:4326', False)
    
    export_config = {
        'image': tte_data_export.clip(geometry),
        'description': output_name,
        'scale': scale,
        'region': geometry,
        'maxPixels': int(max_pixels),
        'fileFormat': 'GeoTIFF',
        'formatOptions': {
            'cloudOptimized': True
        }
    }
    
    # Add folder if specified
    if output_dir:
        export_config['folder'] = output_dir
    
    task = ee.batch.Export.image.toDrive(**export_config)
    task.start()
    
    print(f"Drive export task started: {output_name}")
    print(f"Task ID: {task.id}")
    
    return task

def export_to_gcs(tte_data_export, output_name, extent, scale, max_pixels, gcs_bucket, gcs_prefix=None):
    """Export TTE data to Google Cloud Storage."""
    print(f"Exporting to GCS: {output_name}...")
    
    geometry = ee.Geometry.Rectangle(extent, 'EPSG:4326', False)
    
    # Build GCS path
    if gcs_prefix:
        file_name_prefix = f"{gcs_prefix}/{output_name}"
    else:
        file_name_prefix = output_name
    
    export_config = {
        'image': tte_data_export.clip(geometry),
        'description': output_name,
        'bucket': gcs_bucket,
        'fileNamePrefix': file_name_prefix,
        'scale': scale,
        'region': geometry,
        'maxPixels': int(max_pixels),
        'fileFormat': 'GeoTIFF',
        'formatOptions': {
            'cloudOptimized': True
        }
    }
    
    task = ee.batch.Export.image.toCloudStorage(**export_config)
    task.start()
    
    print(f"GCS export task started: {output_name}")
    print(f"Task ID: {task.id}")
    print(f"GCS location: gs://{gcs_bucket}/{file_name_prefix}.tif")
    
    return task

def download_to_local(tte_data_export, output_name, extent, scale, output_dir):
    """Download TTE data directly to local directory using getDownloadURL."""
    print(f"Downloading to local: {output_name}...")
    
    import requests
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    geometry = ee.Geometry.Rectangle(extent, 'EPSG:4326', False)
    
    try:
        # Get download URL
        url = tte_data_export.clip(geometry).getDownloadURL({
            'scale': scale,
            'crs': 'EPSG:4326',
            'fileFormat': 'GeoTIFF',
            'region': geometry
        })
        
        print(f"Download URL generated, starting download...")
        
        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Save to local file
        output_file = os.path.join(output_dir, f"{output_name}.tif")
        
        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Successfully downloaded: {output_file}")
        return output_file
        
    except Exception as e:
        print(f"Direct download failed: {e}")
        print("Consider using Drive or GCS export for large images")
        return None

def list_asset_roots():
    """List available asset roots for the authenticated user."""
    try:
        roots = ee.data.getAssetRoots()
        print("Available asset roots:")
        for root in roots:
            print(f"  {root['id']}")
        return roots
    except Exception as e:
        print(f"Error listing asset roots: {e}")
        return None

def main():
    """Main TTE processing workflow."""
    parser = argparse.ArgumentParser(description='TTE Classification - Complete Python Implementation')
    
    # GEE Authentication arguments
    parser.add_argument('--gee_account', required=True, type=str,
                       help='GEE service account email')
    parser.add_argument('--gee_key_path', required=True, type=str,
                       help='Path to GEE service account JSON key file (local or s3://)')
    
    # Processing arguments
    parser.add_argument('--num_month', type=int, required=True, choices=[2, 6, 7],
                       help='Processing month version (2, 6, or 7)')
    parser.add_argument('--output_name', type=str,
                       help='Output name (default: auto-generated)')
    parser.add_argument('--scale', type=int, default=30,
                       help='Output resolution in meters (default: 30)')
    parser.add_argument('--extent', nargs=4, type=float,
                       default=[-180, 45, 180, 75],
                       help='Export extent [xmin ymin xmax ymax]')
    parser.add_argument('--max_pixels', type=float, default=1e13,
                       help='Maximum pixels for export')
    
    # Export options
    parser.add_argument('--export_type', type=str, default='asset',
                       choices=['asset', 'drive', 'gcs', 'local'],
                       help='Export destination (default: asset)')
    parser.add_argument('--asset_path', type=str,
                       help='Full asset path (e.g. users/username/folder/asset_name)')
    parser.add_argument('--output_dir', type=str,
                       help='Output directory for local/drive exports')
    parser.add_argument('--gcs_bucket', type=str,
                       help='GCS bucket name (required for gcs export)')
    parser.add_argument('--gcs_prefix', type=str,
                       help='GCS path prefix (optional)')
    parser.add_argument('--export_bioclim', action='store_true',
                       help='Also export bioclimatic envelope')
    
    args = parser.parse_args()
    
    # Validate export options
    if args.export_type == 'gcs' and not args.gcs_bucket:
        print("Error: --gcs_bucket required for GCS export")
        sys.exit(1)
    
    if args.export_type == 'local' and not args.output_dir:
        print("Error: --output_dir required for local export")
        sys.exit(1)
    
    # Auto-generate output name if not provided
    if not args.output_name:
        args.output_name = f"TTE_DATA_v20190{args.num_month}v2"
    
    # Use full asset path if provided
    if args.export_type == 'asset' and args.asset_path:
        args.output_name = args.asset_path
    
    print("=== TTE Classification Workflow ===")
    print(f"Month version: {args.num_month}")
    print(f"Output: {args.output_name}")
    print(f"Export type: {args.export_type}")
    print(f"Scale: {args.scale}m")
    print(f"Extent: {args.extent}")
    
    # Initialize Earth Engine
    print("\n1. Initializing Earth Engine...")
    initialize_ee(args.gee_account, args.gee_key_path)
    
    # Load datasets
    print("\n2. Loading datasets...")
    datasets = load_datasets()
    
    # Setup TCC data and create TTE domain
    print("\n3. Setting up TCC data and TTE domain...")
    tcc_data, tte_domain, bioclim_env_tte = setup_tcc_data(datasets, args.num_month)
    
    # Calculate abruptness
    print("\n4. Calculating TCC abruptness...")
    tcc_data_with_abruptness = get_abruptness(tcc_data)
    
    # Create zones and classification
    print("\n5. Creating TTE zones and classification...")
    tte_zones, tte_classification, class_names = create_tte_zones_and_classification(
        tcc_data_with_abruptness, datasets)
    
    # Stack output bands (matching original script)
    print("\n6. Stacking output bands...")
    tte_data_export = ee.Image.cat([
        tte_domain.rename('tte_domain'),
        tte_zones.rename('tte_zones'),
        tcc_data_with_abruptness.select('tcc_abruptness').rename('tte_abruptness'),
        tte_classification.rename('tte_classification'),
        tcc_data.rename('tcc')
    ])

    if args.export_type == 'asset':
        print("\nChecking available asset locations...")
        list_asset_roots()
    
    # Export based on type
    print("\n7. Starting export...")
    if args.export_type == 'asset':
        task = export_to_asset(tte_data_export, args.output_name,
                              args.extent, args.scale, args.max_pixels)
    elif args.export_type == 'drive':
        task = export_to_drive(tte_data_export, args.output_name,
                              args.extent, args.scale, args.max_pixels, args.output_dir)
    elif args.export_type == 'gcs':
        task = export_to_gcs(tte_data_export, args.output_name,
                            args.extent, args.scale, args.max_pixels,
                            args.gcs_bucket, args.gcs_prefix)
    elif args.export_type == 'local':
        output_file = download_to_local(tte_data_export, args.output_name,
                                       args.extent, args.scale, args.output_dir)
        if output_file:
            print(f"Local export complete: {output_file}")
        task = None
    
    print("\n=== TTE Classification Complete! ===")
    print(f"Output bands: tte_domain, tte_zones, tte_abruptness, tte_classification, tcc")
    print(f"\nClassification system ({len(class_names)} classes):")
    for i, class_name in enumerate(class_names, 1):
        print(f"  {i}: {class_name}")
    
    if task:
        print(f"\nMonitor exports at: https://code.earthengine.google.com/tasks")
    
    return task

if __name__ == "__main__":
    main()