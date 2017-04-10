#get path
pth1='/home/tpmaxwel/.cdas/cache/cdscan/merra_mon_ua.xml'
#pth1='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/ua.ncml'
#pth2='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA/mon/atmos/ua.ncml'
# variable
varin='ua'
#Reanalysis 2
plot_title='MERRA zonal averaged DJF U winds'
###########################   END of SETUP    ###############
#start timing
import time
start=time.time()
import cdms2, vcs, genutil, cdutil

# Import cdms2
import cdms2, cdutil
import vcs, EzTemplate, sys

cdmsfile=cdms2.open(pth1)
cl = cdmsfile(varin)

print " Running " + plot_title
#Set Bounds For Monthly Dava
cdutil.times.setTimeBoundsMonthly(cl)
##Calculate Climatology DJF for this case
cl_djfclimatology = cdutil.times.DJF.climatology(cl)
#Calculate zonal average
cl_djfclimatology = cdutil.averager(cl_djfclimatology, axis=3,weights='equal')
#Done with calculation

## PLotting set up from here on down:

end=time.time()
print "Finished, execution time = " + str( (end-start) )