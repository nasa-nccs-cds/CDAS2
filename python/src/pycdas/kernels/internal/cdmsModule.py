from pycdas.kernels.Kernel import CDMSKernel, Kernel, KernelSpec
from pycdas.cdasArray import cdmsArray
import cdms2, time, os, cdutil
from pycdas.messageParser import mParse
from regrid2 import Horizontal


class RegridKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("regrid", "Regridder", "Regrids the inputs using UVCDAT", parallize=True ) )
        self._debug = True

    def executeOperations(self, task, _inputs):
        cdms2.setAutoBounds(2)
        crsSpec = task.metadata.get("crs","")
        if( len(crsSpec) and (crsSpec[0] == '~') ):
            crsId = crsSpec[1:]
            grid_input = _inputs.get( crsId, None )
            if not grid_input: raise Exception( "Can't find grid variable uid: " + crsId + ", variable uids = " + str( _inputs.keys() ) )
            toGrid = grid_input.getGrid()
        else:
            crsToks = crsSpec.split('~')
            if( len(crsToks) > 1 ):
                if crsToks[0] == "gaussian":
                    resolution = int(crsToks[1])
                    toGrid = cdms2.createGaussianGrid( resolution )
                else: raise Exception( "Unrecognized grid type: " + crsToks[0])
            else: raise Exception( "Can't find crs spec in regrid kernel: " + str(crsToks))

        results = []
        for input_id in task.inputs:
            _input = _inputs.get( input_id.split('-')[0] )
            variable = _input.getVariable()
            ingrid = _input.getGrid()
            inlatBounds, inlonBounds = ingrid.getBounds()
            self.logger.info( " >> in LAT Bounds shape: " + str(inlatBounds.shape) )
            self.logger.info( " >> in LON Bounds shape: " + str(inlonBounds.shape) )
            outlatBounds, outlonBounds = toGrid.getBounds()
            self.logger.info( " >> out LAT Bounds shape: " + str(outlatBounds.shape) )
            self.logger.info( " >> out LON Bounds shape: " + str(outlonBounds.shape) )
            if( not ingrid == toGrid ):
                self.logger.info( " Regridding Variable {0} using grid {1} ".format( variable.id, str(toGrid) ) )
                if self._debug:
                    self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) ) )
                    self.logger.info( " >> Input Variable Shape: {0}, Grid Shape: {1} ".format( str(variable.shape), str([len(ingrid.getLatitude()),len(ingrid.getLongitude())] )))
                    self.logger.info( " >>  Grid Lat axis: " + str( ingrid.getLatitude()) )
                    self.logger.info( " >>  Grid Lon axis: " + str( ingrid.getLongitude()) )
                    self.logger.info( " >>  in Grid Lat bounds: " + str(inlatBounds) )
                    self.logger.info( " >>  in Grid Lon bounds: " + str(inlonBounds) )
                    self.logger.info( " >>  out Grid Lat bounds: " + str(outlatBounds) )
                    self.logger.info( " >>  out Grid Lon bounds: " + str(outlonBounds) )

                regridFunction = Horizontal(ingrid, toGrid)
                result_var = regridFunction( variable )
                self.logger.info( " >> Gridded Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) ) )
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
