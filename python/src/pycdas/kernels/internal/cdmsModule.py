from pycdas.kernels.Kernel import Kernel, KernelSpec
from pycdas.cdasArray import cdmsArray
import cdms2, time, os, cdutil
from pycdas.messageParser import mParse
from regrid2 import Horizontal

class CDMSKernel(Kernel):

    def createResult(self, result_var, input, task ):
        rv = None
        result_var.id = result_var.id  + "-" + task.rId
        gridFilePath = self.saveGridFile( result_var.id, result_var )
        if( gridFilePath ): result_var.createattribute( "gridfile", gridFilePath )
        result_var.createattribute( "origin", input.origin )
        result = cdmsArray.createResult( task, input, result_var )
        if self.cacheReturn[0]: self.cached_results[ result.id ] = result
        if self.cacheReturn[1]: rv = result
        return rv

class RegridKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("regrid", "Regridder", "Regrids the inputs using UVCDAT", parallize=True ) )
        self._debug = False

    def executeOperations(self, task, _inputs):
        crsToks = task.metadata.get("crs","").split("~")
        if not crsToks[0]: raise Exception( "Can't find crs spec in regrid kernel")
        if( len(crsToks) > 1 ):
            if crsToks[0] == "gaussian":
                resolution = int(crsToks[1])
                toGrid = cdms2.createGaussianGrid( resolution )
            else: raise Exception( "Unrecognized grid type: " + crsToks[0])
        else:
            grid_inputs = [ input for input in _inputs if( input.uid() == crsToks[0] ) ]
            if len( grid_inputs ) == 0: raise Exception( "Can't find grid variable: " + crsToks[0])
            toGrid = grid_inputs[0].getGrid()

        results = []
        for _input in _inputs:
            variable = _input.getVariable()
            ingrid = variable.getGrid()
            if( not ingrid == toGrid ):
                regridFunction = Horizontal(ingrid, toGrid)
                inlatBounds, inlonBounds = ingrid.getBounds()
                self.logger.info( " Regridding Variable {0} using grid {1} ".format( variable.id, str(toGrid) ) )
                result_var = regridFunction( variable )
                if self._debug:
                    self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) ) )
                    self.logger.info( " >> Input Variable Shape: {0}, Grid Shape: {1} ".format( str(variable.shape), str([len(ingrid.getLatitude()),len(ingrid.getLongitude())] )))
                    self.logger.info( " >>  Grid Lat axis: " + str( ingrid.getLatitude()) )
                    self.logger.info( " >>  Grid Lon axis: " + str( ingrid.getLongitude()) )
                    self.logger.info( " >>  Grid Lat bounds: " + str(inlatBounds) )
                    self.logger.info( " >>  Grid Lon bounds: " + str(inlonBounds) )
                    self.logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) ) )
                results.append( self.createResult( result_var, _input, task ) )
        return results

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting by default", parallize=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        variable = _input.getVariable()
        axis = task.metadata.get("axis","xy")
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axis, weights=weights, action=action, returned=returned )
        return self.createResult( result_var, _input, task )

if __name__ == "__main__":
    from cdms2 import timeslice
    dsetUri = "http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_195101-200512.nc"
    resolution = 128
    dset = cdms2.open(dsetUri)
    variable = dset["tas"](timeslice(0,1))
    ingrid = variable.getGrid()

    axis = "xy"
    weights = "generate"
    action = "average"
    returned = 0
    result_var = cdutil.averager( variable, axis=axis, weights=weights, action=action, returned=returned )
    outgrid = result_var.getGrid()

    print "."


    # t42 = cdms2.createGaussianGrid( resolution )
    # regridFunction = Horizontal( ingrid, t42)
    # print " Ingrid: " + str(ingrid)
    # print " Variable: " + str(variable)
    # inlatBounds, inlonBounds = ingrid.getBounds()
    # print " >>  Grid Lat bounds: " + str(inlatBounds)
    # print " >>  Grid Lon bounds: " + str(inlonBounds)
    # print " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) )
    # result_var = regridFunction( variable )
    # print " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) )
