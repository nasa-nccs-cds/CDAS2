#!/usr/bin/env bash

datafile="../data/MERRA_TEST_DATA_ta.nc"

ncwa -O -v ta -a time -d lat,0.,0. -d lon,0.,0. ${datafile} out/tave.nc

ncdiff -O -y sbt -d lat,0.,0. -d lon,0.,0. ${datafile} out/tave.nc out/tanom.nc
