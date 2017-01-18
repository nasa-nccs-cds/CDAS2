from pycdas.kernels.Kernel import Kernel, KernelSpec
from pycdas.cdasArray import cdmsArray
import cdms2, time, os
from pycdas.messageParser import mParse
from regrid2 import Horizontal

class RegridKernel(Kernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("regrid", "Regridder", "Regrids the inputs using UVCDAT", parallize=True ) )

    def executeOperation(self, task, _input):
        t0 = time.time()
        cacheReturn = [ mParse.s2b(s) for s in task.metadata.get( "cacheReturn", "ft" ) ]
        variable = _input.getVariable()
        crsToks = task.metadata.get("crs","gaussian~128").split("~")
        regridder = task.metadata.get("regridder","regrid2")
        crs = crsToks[0]
        resolution = int(crsToks[1]) if len(crsToks) > 1 else 128
        if crs == "gaussian":
            rv = None
            t42 = cdms2.createGaussianGrid( resolution )
            self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) ) )
            ingrid = variable.getGrid()
            self.logger.info( " >> Input Variable Shape: {0}, Grid Shape: {1} ".format( str(variable.shape), str([len(ingrid.getLatitude()),len(ingrid.getLongitude())] )))
            regridFunction = Horizontal(ingrid, t42)
            self.logger.info( " >>  Grid Lat axis: " + str( ingrid.getLatitude()) )
            self.logger.info( " >>  Grid Lon axis: " + str( ingrid.getLongitude()) )
            inlatBounds, inlonBounds = ingrid.getBounds()
            self.logger.info( " >>  Grid Lat bounds: " + str(inlatBounds) )
            self.logger.info( " >>  Grid Lon bounds: " + str(inlonBounds) )
            result_var = regridFunction( variable )
            result_var.id = result_var.id  + "-" + task.rId
            self.logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) ) )
            gridFilePath = self.saveGridFile( result_var.id, result_var )
            result_var.createattribute( "gridfile", gridFilePath )
            result_var.createattribute( "origin", variable.attributes[ "origin"] )
            result = cdmsArray.createResult( task, _input, result_var )
            if cacheReturn[0]: self.cached_results[ result.id ] = result
            if cacheReturn[1]: rv = result
            self.logger.info( " >> Regridded variable in time {0}".format( (time.time()-t0) ) )
            return rv


if __name__ == "__main__":
    from cdms2 import timeslice
    dsetUri = "http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_195101-200512.nc"
    resolution = 128
    dset = cdms2.open(dsetUri)
    variable = dset["tas"](timeslice(0,1))
    ingrid = variable.getGrid()
    t42 = cdms2.createGaussianGrid( resolution )
    regridFunction = Horizontal( ingrid, t42)
    print " Ingrid: " + str(ingrid)
    print " Variable: " + str(variable)
    inlatBounds, inlonBounds = ingrid.getBounds()
    print " >>  Grid Lat bounds: " + str(inlatBounds)
    print " >>  Grid Lon bounds: " + str(inlonBounds)
    print " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) )
    result_var = regridFunction( variable )
    print " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) )
