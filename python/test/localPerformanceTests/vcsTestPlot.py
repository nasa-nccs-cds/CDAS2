import cdms2, vcs
# dataPath = "/home/tpmaxwel/.cdas/cache/cdscan/merra2_mon_ua.xml"
dataPath = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/atmos_ua.nc"
varName = "ua"
f = cdms2.openDataset(dataPath)
var = f.variables[varName]
s = var[0]
x = vcs.init()
x.plot(s,variable = var)
x.interact()
