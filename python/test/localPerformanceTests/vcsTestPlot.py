def nbDisplay( x ):
    from IPython.display import Image, display
    import os
    outFile = '/tmp/vcsnb-{0}.png'.format( os.getpid() )
    x.png(outFile)
    display( Image(outFile) )

import cdms2, vcs
dataPath = "/home/tpmaxwel/.cdas/cache/cdscan/merra2_mon_ua.xml"
varName = "ua"
f = cdms2.openDataset(dataPath)
var = f( varName, time=slice(0,1),level=slice(10,11) )
s = var[0]
x = vcs.init()
x.plot(s,variable = var)
nbDisplay(x)




