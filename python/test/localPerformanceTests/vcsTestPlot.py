import os, random, string, cdms2, vcs

def random_id( length ): ''.join(random.choice( string.ascii_lowercase + string.ascii_uppercase + string.digits ) for _ in range(length))

def nbDisplay( x ):
    from IPython.display import Image, display
    outFile = '/tmp/vcsnb-{0}.png'.format( random_id(8) )
    x.png(outFile)
    display( Image(outFile) )

dataPath = "/home/tpmaxwel/.cdas/cache/cdscan/merra2_mon_ua.xml"
varName = "ua"
f = cdms2.openDataset(dataPath)
var = f( varName, time=slice(0,1),level=slice(10,11) )
s = var[0]
x = vcs.init()
x.plot(s,variable = var,bg=True)




