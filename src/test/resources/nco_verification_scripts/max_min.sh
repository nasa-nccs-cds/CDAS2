#!/usr/bin/env bash

# datafile="../data/MERRA_TEST_DATA.ta.nc"
datafile="../data/ConstantTestData.ta.nc"

ncwa -O -d time,"1979-01-16T12:00:00Z","1979-01-16T12:00:00Z" -a lat,lon -y max ${datafile} out/maxval.nc
ncwa -O -d time,"1979-01-16T12:00:00Z","1979-01-16T12:00:00Z" -a lat,lon -y min ${datafile} out/minval.nc
ncwa -O -d time,"1979-01-16T12:00:00Z","1979-01-16T12:00:00Z" -a lat,lon -y sum ${datafile} out/sumval.nc
