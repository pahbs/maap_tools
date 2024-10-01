from pathlib import Path
import geopandas as gpd
import numpy as np
from geopandas import GeoDataFrame
from geopandas.tools import sjoin
import pandas as pd
import pyproj
import shapely

import glob
import os
import random 
import shutil
import time

import rasterio as rio
from rasterio.coords import BoundingBox
from rasterio.coords import disjoint_bounds

import rasterstats
from rasterstats import point_query

def do_s3_point_query(s3_url, gdf, raster_data_name, bandnum=1, ANON=True, PROFILE_NAME='boreal_pub', DEBUG=False):
    import rasterio
    if ANON:
        session = rasterio.env.Env(AWS_NO_SIGN_REQUEST='YES')
    else:
        session = rasterio.env.Env(profile_name=PROFILE_NAME)
    with session:
        with rasterio.open(s3_url, mode='r') as dataset:
            if DEBUG:
                print(f'raster dataset indexes: {dataset.indexes}')
            new_gdf = reproject_gdf_to_rio_ds(gdf, dataset)
            coord_list = [(x,y) for x,y in zip(new_gdf['geometry'].x , new_gdf['geometry'].y)]
            if DEBUG:
                print(coord_list)
                
            # Sample the dataset at this point
            # return the first value associated with the input bandnum
            new_gdf[raster_data_name] = [x for x in dataset.sample(coord_list)][0][bandnum-1]
            if DEBUG:
                print(new_gdf.columns)
            return new_gdf

# def rio_open_aws_r(session, url, gdf):
#     with session:
#         with rio.open(url) as ras:
#             new_gdf = reproject_gdf_to_rio_ds(gdf, ras)
#             new_gdf[ras_dict['data_name']] = do_point_query(new_gdf, ras_fn) 
#             return new_gdf

def do_point_query(new_gdf, ras_fn):
    return point_query(new_gdf,ras_fn, interpolate='nearest')

def get_transformation(ras_fn, gdf):
    ras=rio.open(ras_fn)
    ras_crs=ras.crs
    ras.close()
    return pyproj.Transformer.from_crs(gdf.crs, ras_crs, always_xy=True).transform

def reproject_gdf_to_rio_ds(gdf, rio_ds):
        ras_crs=rio_ds.crs
        gdf_crs=gdf.crs
        return gdf.to_crs(ras_crs)

def clip_and_join(gdf,ras_gdf):
    """
    When working with the tiled raster this function is called to remove the raster tiles from the raster_gdf that don't overlap the points. 
    The Raster paths are then joined with the (point) gdf to get the raster tile path corresponding to each point.
    All functions assume raster path info is stored in column named "location"
    """
    overlaps=gpd.clip(ras_gdf, gdf,keep_geom_type=True)
    gdf=gpd.sjoin(gdf,overlaps, how="left")
    gdf.sindex
    overlaps=None
    has_joined=True
    return gdf,has_joined

def ExtractUntiledRaster(gdf,ras_dict):
    """For untiled rasters, the gdf just needs to be reprojected to the raster projection, then the point_query function takes in 
    the reprojected gdf and the path to the global raster (eg. Worldclim files)"""
    
    if ras_dict['location'] == 'local':
        print("Raster is an ADAPT untiled raster")
        #Open Raster from local source
        ras_fn=os.path.join(ras_dict['data_dir'],ras_dict['file_name'])
        ras=rio.open(ras_fn)
        new_gdf = reproject_gdf_to_rio_ds(gdf, ras)
        new_gdf[ras_dict['data_name']] = do_point_query(new_gdf, ras_fn) #Main command to extract ras values. For untiled takes reprojected df and global raster path
        return new_gdf

    elif ras_dict['location'] == 's3':
        print("Raster is an S3 untiled raster")
        session = rasterio.env.Env(profile_name='')
        url = os.path.join(ras_dict['data_dir'],ras_dict['file_name'])
        with session:
            with rio.open(url) as ras:
                new_gdf = reproject_gdf_to_rio_ds(gdf, ras)
                new_gdf[ras_dict['data_name']] = do_point_query(new_gdf, ras_fn) 
                return new_gdf
        
def ExtractTiledRaster(gdf,ras_dict, tcc_year=None, bandnum=1, DEBUG=False):
    
    """ For the tiled rasters, the point_query function must work on a point by point basis based on the column in the gdf pointing to the
    raster location. Each point is reprojected to the specific raster underneath it, and point_query takes in the reprojected point geom
    and the path to the raster tile"""
    
    if ras_dict['location'] == 'local':
        print("Raster is an ADAPT tiled raster")
        n=0
        nnan=0
        value_list=[]
        for idx,point in gdf.iterrows():
            n+=1
            if type(point.location) is float:
                value_list.append(np.nan)
                nnan+=1
            else:
                ras_fn=os.path.join(ras_dict['data_dir'],str(point.location))

                new_point_geom=shapely.ops.transform(get_transformation(ras_fn, gdf), point.geometry)
                value = do_point_query(new_point_geom, ras_fn) #Main command to extract ras value. For tiled raster works point by point. Takes reprojected point geom and raster path
                value_list.append(value)
            
        gdf[ras_dict['data_name']]=value_list
        print("There were {} NaN filenames out of {}".format(nnan,n))
        
    elif ras_dict['location'] == 's3':
        if DEBUG:
            print("Raster is an S3 tiled raster")
            print(f"GDF shape: {gdf.shape}")
        new_gdf_list = []
        col_name = ras_dict['data_name']
        for idx,point in gdf.iterrows():
            if DEBUG:
                print(point.index1)
            if isinstance(point.s3_path, str):
                ras_fn = point.s3_path
                #col_name = ras_dict['data_name']

                if tcc_year is not None:
                    if DEBUG:
                        print(ras_fn)
                    ras_fn = ras_fn.replace('y2020', 'y'+str(tcc_year))
                    col_name = ras_dict['data_name'].replace('tcc2020', 'tcc'+str(tcc_year))
                    
                if DEBUG:
                    #print(gdf.iloc[[idx]])
                    print(ras_fn)
                    print(col_name)
                    print(gdf.columns)
                    
                new_gdf = do_s3_point_query(ras_fn, gdf.iloc[[idx]], col_name, ANON=True, bandnum=bandnum, DEBUG=DEBUG)
                
                if DEBUG:
                    print(new_gdf.columns)
            else:
                # This adds nan to a point that is outside of the raster
                new_gdf = gdf.iloc[[idx]]
                new_gdf[col_name] = np.nan  
                if DEBUG:
                    print('Point is outside of raster extent...')
                    print(new_gdf.columns)
                #continue
                
            new_gdf_list.append(new_gdf.to_crs(gdf.crs))
            
        gdf = pd.concat(new_gdf_list)
    
    return gdf

def run_extract_tiled_covar_year(input_points_gdf, raster_dict=None, tcc_year=None, bandnum=1, has_joined=False, DEBUG=False):
    
    '''
    ## Function to extract a tiled covar by year
    
    Use for time series stored as similarly name individual rasters, as well as any indiv raster
    Uses year arg to specify the s3 path to the year-specific tile
    
    Function formulated to extract yearly TCC raster values on s3 to points in a geodataframe
    
    ras_dict : a dictionary of tcc2020 raster location, name, and raster footprint gpkg path
    input_points_gdf : a geodataframe with geometry that will be reprojected to that of raster tile using the raster footprint geodataframe
    has_joined : if True then no clipping
    
    note : this function can be multithreaded like this:
    from multiprocessing import Pool
    from functools import partial

    with Pool(processes=10) as pool:
        pool.map(partial(run_extract_tiled_raster, raster_dict=<a_dict>, tcc_year=<a_year>), input_points_gdf_list)
    '''
    if tcc_year is None:
        print(f"Extracting from {raster_dict['data_name']}...")
    else:
        print(f'Extracting TCC from {tcc_year}...')
    if raster_dict is None:
        print('Input raster dict is None.')
        return None
    
    #print("\nRunning point extraction for tiled tcc raster for year {}".format(str(tcc_year)))
    if has_joined:
        pass
    else:
        #print("\n\tClipping to get tiles overlapping points and spatially joining with footprint tile extent to get tile path...")
        footprint_gdf = gpd.read_file(raster_dict['footprint_fn'])
        joined_points_gdf, has_joined = clip_and_join(input_points_gdf.to_crs(footprint_gdf.crs), footprint_gdf)
    
        print(f"Shape of subset of gdf with no s3_path : {joined_points_gdf[~joined_points_gdf['s3_path'].isnull()].shape}")
    if False:
        # Take out the points that didnt overlay with a tile, and reset index
        extracted_points_gdf = ExtractTiledRaster(joined_points_gdf[~joined_points_gdf['s3_path'].isnull()].reset_index(), raster_dict, tcc_year=tcc_year, bandnum=bandnum, DEBUG=DEBUG)
    else:
        print(f"Shape of gdf : {joined_points_gdf.shape}")

        # Take out the points that didnt overlay with a tile, and reset index
        extracted_points_gdf = ExtractTiledRaster(joined_points_gdf.reset_index(), raster_dict, tcc_year=tcc_year, bandnum=bandnum, DEBUG=DEBUG)

    return extracted_points_gdf