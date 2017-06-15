#!/usr/bin/env bash

datafile="http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc"
datafile1="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/JAN/MERRA300.prod.assim.inst3_3d_asm_Cp.20050101.SUB.nc"
# mkdir -p out

# ncks -O -v tas -d lat,0,5 -d lon,0,5 -d time,0,0 ${datafile} ~/test/out/subset_xi05_yi05_ti0_GISS_r1i1p1_185001-190012.nc

# ncks -O -v tas -d lat,0.,0. -d lon,0.,0. ${datafile} out/subset_xy00_GISS_r1i1p1_185001-190012.nc


# ncwa -O -v tas -d time,10,10 -a lat,lon -y max ${datafile} ~/test/out/maxval.nc
# ncwa -O -v tas -d time,10,10 -a lat,lon -y min ${datafile} ~/test/out/minval.nc

# ncwa -O -v tas -d time,10,10 -a lat,lon -y sum ${datafile} ~/test/out/sumval.nc

# ncwa -O -v tas -d lat,5,8 -d lon,5,8 -d time,0,100 -a time -y min ${datafile} ~/test/out/minval.nc

# ncwa -O -v tas -d lat,5,8 -d lon,5,8 -d time,50,150 -a time -y min ${datafile} maxval.nc

# ncwa -O -v tas -d time,10,10 -d lat,10,20 -a lat,lon -y max ${datafile} maxval.nc

# ncwa -O -v t -d time,4,4 -d levels,10,10 -a latitude,longitude -y min ${datafile1} maxval.nc

# ncwa -O -v tas -d lat,5,5 -d lon,5,10 -a time -y sum ${datafile} tsumval.nc

# ncwa -O -v tas -d time,10,10 -a lat,lon -y max ${datafile} ~/test/out/maxval.nc
# ncdump ~/test/out/maxval.nc

#"""[domain=[{"name":"d0","lat":{"start":30,"end":40,"system":"values"},"time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"x"}]]"""


ncks -O -v tas  -d lat,5,5 -d lon,5,10 ${datafile} ~/test/out/sample_data.nc
ncwa -O -v tas -a time ~/test/out/sample_data.nc ~/test/out/time_ave.nc
ncbo -O -v tas ~/test/out/sample_data.nc ~/test/out/time_ave.nc ~/test/out/dev.nc
ncra -O -y rmssdn  ~/test/out/dev.nc ~/test/out/stdev.nc
