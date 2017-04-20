#get path
pth1='/home/tpmaxwel/.cdas/cache/cdscan/merra_mon_ua.xml'
locpth = '/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/atmos_ua.nc'
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

print "Plotting " + plot_title

cdmsfile=cdms2.open(pth1)
cl = cdmsfile(varin)

#Set Bounds For Monthly Dava
cdutil.times.setTimeBoundsMonthly(cl)
##Calculate Climatology DJF for this case
cl_djfclimatology = cdutil.times.DJF.climatology(cl)
#Calculate zonal average
cl_djfclimatology = cdutil.averager(cl_djfclimatology, axis=3,weights='equal')

print "Done with calculation, begin plotting"

## PLotting set up from here on down:

x=vcs.init()
cl_djfclimatology=cl_djfclimatology(squeeze=1)

levs=[-60,-50,-40,-30,-25,-20,-15,-10,-5,0,5,10,15,20,25,30,40,50,60]
iso = x.createisofill()
iso.levels=levs
iso.missing='grey'
cl_djfclimatology.id=''
x.drawlogooff()
bg=False
M=EzTemplate.Multi(rows=1,columns=1)
M.legend.direction='horizontal'
M.margins.left=.07
M.margins.right=.07
M.margins.bottom=.1
M.margins.top=.08
M.legend.thickness=.3
M.spacing.vertical=.1
M.legend.fat=.09
x.setcolormap('bl_to_darkred')
t=M.get(legend='local')
lines=vcs.createisoline()
header=x.createtext()
header.To.height=24
header.To.halign="center"
header.To.valign="top"
header.x=.5
header.y=.98
header.string=plot_title
x.plot(header,bg=1)
iso.yticlabels1={100000:"1000",90000:"900",80000:"800",70000:"700",60000:"600",50000:"500",40000:"400",30000:"300",20000:"200",10000:"100"}
x.plot(cl_djfclimatology,t, iso)
lines.yticlabels1={100000:"1000",90000:"900",80000:"800",70000:"700",60000:"600",50000:"500",40000:"400",30000:"300",20000:"200",10000:"100"}
lines.levels=levs
x.plot(cl_djfclimatology,t,lines)
lines=vcs.createisoline()
lines.levels=levs

end=time.time()
print " Plot constructed, total time = {0} sec".format( str(end-start) )

x.interact()