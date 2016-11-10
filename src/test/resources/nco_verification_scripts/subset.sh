#!/usr/bin/env bash

#datafile="giss_r1i1p2.xml"
#datafile="http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p2/tas_Amon_GISS-E2-H_historical_r1i1p2_185001-195012.nc"
datafile="http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p2/tas_Amon_GISS-E2-H_historical_r1i1p2_185001-195012.nc"

ncks -O -v tas -d lat,0.,0. -d lon,0.,0. ${datafile} out/subset00_GISS_r1i1p2_185001-195012.nc