#!/usr/bin/env bash

datafile="../data/MERRA_TEST_DATA_ta.nc"

ncks -O -v ta -d lat,0.,0. -d lon,0.,0. ${datafile} out/ta_subset_0_0.nc

