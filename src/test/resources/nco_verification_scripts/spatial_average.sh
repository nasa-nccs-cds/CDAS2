#!/usr/bin/env bash

#datafile="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA_TEST_DATA.ta.nc"
#datafile="http://dataserver.nccs.nasa.gov/thredds/ncss/bypass/CREATE-IP/MERRA/mon/atmos/ta.ncml?var=ta&disableLLSubset=on&disableProjSubset=on&horizStride=1&time_start=1979-01-16T12%3A00%3A00Z&time_end=2014-12-16T12%3A00%3A00Z&timeStride=1&vertCoord=75000.0"

datafile="../data/MERRA_TEST_DATA.ta.nc"

ncwa -O -d time,"1979-01-16T12:00:00Z","1979-01-16T12:00:00Z" -a lat,lon ${datafile} out/spatial_average-1.nc
