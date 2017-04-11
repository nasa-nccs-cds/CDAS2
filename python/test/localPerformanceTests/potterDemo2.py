pth1='/home/tpmaxwel/.cdas/cache/cdscan/merra2_mon_ua.xml'
pth2='/home/tpmaxwel/.cdas/cache/cdscan/merra_mon_ua.xml'
varin='ua'
model1_name='MERRA2'
model2_name='MERRA'
outfile='/home/tpmaxwel/.cdas/cache/'
plot_title='MERRA2 (top) MERRA zonal averaged DJF U winds'
###########################   END of SETUP    ###############

import time
import cdms2, cdutil
import vcs, EzTemplate, sys

start=time.time()
print "Plotting " + plot_title
cdmsfile=cdms2.open(pth1)
cdmsfile2 = cdms2.open(pth2)

cl = cdmsfile(varin)
cl2= cdmsfile2(varin)

#Set Bounds For Monthly Dava
cdutil.times.setTimeBoundsMonthly(cl)
cdutil.times.setTimeBoundsMonthly(cl2)

##Calculate Climatology DJF for this case
cl_djfclimatology = cdutil.times.DJF.climatology(cl)
cl2_djfclimatology = cdutil.times.DJF.climatology(cl2)

#Calculate zonal average
cl_djfclimatology = cdutil.averager(cl_djfclimatology, axis=3,weights='equal')
cl2_djfclimatology = cdutil.averager(cl2_djfclimatology, axis=3,weights='equal')

print "Done with calculation, begin plotting"

x=vcs.init()
cl_djfclimatology=cl_djfclimatology(squeeze=1)
cl2_djfclimatology=cl2_djfclimatology(squeeze=1)
levs=[-60,-50,-40,-30,-25,-20,-15,-10,-5,0,5,10,15,20,25,30,40,50,60]
iso = x.createisofill()
iso.levels=levs
iso.missing='grey'
cl_djfclimatology.id=''
cl2_djfclimatology.id=''
x.drawlogooff()
bg=False
M=EzTemplate.Multi(rows=2,columns=1)
M.legend.direction='horizontal'
#M.legend.direction='horizontal'
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

iso.yticlabels1={100000:"1000",90000:"900",80000:"800",70000:"700",60000:"600",50000:"500",40000:"400",30000:"300",20000:"200",10000:"100"}
cl2_djfclimatology=cl2_djfclimatology(squeeze=1)
cl2_djfclimatology.id=''
iso.levels=levs
t=M.get(legend='local')
x.plot(cl2_djfclimatology,t, iso)
lines.yticlabels1={100000:"1000",90000:"900",80000:"800",70000:"700",60000:"600",50000:"500",40000:"400",30000:"300",20000:"200",10000:"100"}
lines.levels=levs
x.plot(cl2_djfclimatology,t,lines)

end=time.time()
png_file = outfile+'/'+varin+'.png'
print " Plot constructed, total time = {0} sec, png_file = {1}".format( str(end-start), png_file )
x.png(png_file)
x.showGUI()
interactor = x.renWin.GetInteractor()
if interactor:
    print "Running interactor"
    interactor.Start()
else:
    print "Sleeping for 30 sec."
    time.sleep(30)
#x.interact()
