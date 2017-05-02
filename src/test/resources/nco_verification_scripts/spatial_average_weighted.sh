#!/usr/bin/env bash

# datafile="../data/MERRA_TEST_DATA_ta.nc"
#datafile="../data/ConstantTestData.ta.nc"
datafile="http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc"
ncap2 -O -S cosine_weights.nco ${datafile} /tmp/data_with_weights.nc
ncwa -O -w gw -d time,0,10 -a lat,lon /tmp/data_with_weights.nc /tmp/spatial_average-1.nc
ncdump /tmp/spatial_average-1.nc
