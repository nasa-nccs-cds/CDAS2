#!/usr/bin/env bash

# datafile="../data/MERRA_TEST_DATA.ta.nc"
datafile="../data/ConstantTestData.ta.nc"

ncap2 -O -S cosine_weights.nco ${datafile} /tmp/data_with_weights.nc

ncwa -O -w gw -d time,"1979-01-16T12:00:00Z","1979-01-16T12:00:00Z" -a lat,lon /tmp/data_with_weights.nc spatial_average-1.nc
