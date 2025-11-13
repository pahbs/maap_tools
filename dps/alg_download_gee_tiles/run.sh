#!/bin/bash
# this is intended for running DPS jobs; the input directory is where a single file has been pulled because download=TRUE in the algorithm_config.yaml file

# This installs the python libs needed to run the script at the bottom

set -x

source activate python

unset PROJ_LIB

mkdir output
pip install geedim
# FILENAMELIST=$(ls -d input/*)

basedir=$( cd "$(dirname "$0")" ; pwd -P )  # goes to alg_3-1-5/

## Hard coded args for each run (if any; usually just output dir)

# Work dir is always from where your script is called
# Base dir is always the relative dir within the run*.sh script

# Absolute path here
# This PWD is wherever the job is run (where the .sh is called from) 
OUTPUTDIR="${PWD}/output"

python ${basedir}/../../lib/download_gee_tiles.py \
--gee_account ${1} \
--gee_key ${2} \
--asset_path ${3} \
--asset_type ${4} \
--year ${5} \
--scale ${6} \
--tiles_csv ${7} \
--tile_list ${8} \
--resampling ${9} \
--outdir ${OUTPUTDIR}
