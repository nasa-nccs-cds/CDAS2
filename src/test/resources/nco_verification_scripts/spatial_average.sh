#!/usr/bin/env bash

datafile="../data/MERRA_TEST_DATA_ta.nc"

ncwa -O -d time,"1979-01-16T12:00:00Z","1979-01-16T12:00:00Z" -a lat,lon ${datafile} out/spatial_average-1.nc
