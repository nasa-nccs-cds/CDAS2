datafile=/home/tpmaxwel/.cdas/cache/collections/NCML/CIP_MERRA_ua_mon.ncml
variable=ua
ncwa -O -v ${variable} -d time,0,100 -d lat,30,40 -d lon,30,40 -d lev,20,20 -a lat,lon,lev -y max ${datafile} /tmp/maxval.nc
ncdump /tmp/maxval.nc
