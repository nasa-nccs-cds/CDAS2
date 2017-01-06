import cdms2

inputPath = "http://aims3.llnl.gov/thredds/dodsC/cmip5_css02_data/cmip5/output1/CMCC/CMCC-CESM/historical/mon/atmos/Amon/r1i1p1/tas/1/tas_Amon_CMCC-CESM_historical_r1i1p1_198001-198412.nc"

dataset = cdms2.open( inputPath )
resolution = 128
regridder = "regrid2"

input = dataset("tas")

t42 = cdms2.createGaussianGrid( resolution )
result = input.regrid( t42 , regridTool=regridder )

axes = result.getAxisList()
grid = result.getGrid()

newDataset = cdms2.createDataset( "/tmp/test" )
for axis in axes: newDataset.copyAxis(axis)
newDataset.copyGrid(grid)
newDataset.createVariableCopy(result)
newDataset.close()

print "."




