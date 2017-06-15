import os, random, string, cdms2, vcs

dataPath = "/Users/tpmaxwel/.cdas/cdscan/MERRA_DAILY.xml"
varName = "t"
f = cdms2.openDataset(dataPath)
var = f( varName, time=slice(0,1),level=slice(10,11) )
s = var[0]
x = vcs.init()
x.plot(s,variable = var,bg=True)