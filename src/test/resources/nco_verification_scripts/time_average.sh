#!/usr/bin/env bash

datafile="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/MERRA2_200.inst6_3d_ana_Np.20000101.nc4"
ncwa -O -v T -d lat,10,10 -d lon,20,20 -a time ${datafile} ~/test/out/time_ave.nc
ncdump ~/test/out/time_ave.nc
