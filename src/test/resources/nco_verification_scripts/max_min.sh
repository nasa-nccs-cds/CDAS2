#!/usr/bin/env bash

datafile="http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc"
mkdir -p out

# ncks -O -v tas -d lat,0,5 -d lon,0,5 -d time,0,0 ${datafile} ~/test/out/subset_xi05_yi05_ti0_GISS_r1i1p1_185001-190012.nc

# ncks -O -v tas -d lat,0.,0. -d lon,0.,0. ${datafile} out/subset_xy00_GISS_r1i1p1_185001-190012.nc


ncwa -O -v tas -d time,10,10 -a lat,lon -y max ${datafile} ~/test/out/maxval.nc
# ncwa -O -v tas -d time,10,10 -a lat,lon -y min ${datafile} ~/test/out/minval.nc

# ncwa -O -v tas -d time,10,10 -a lat,lon -y sum ${datafile} ~/test/out/sumval.nc
