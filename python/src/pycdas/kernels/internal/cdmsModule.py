from pycdas.kernels.Kernel import CDMSKernel, Kernel, KernelSpec
from pycdas.cdasArray import cdmsArray
import cdms2, time, os, cdutil
from pycdas.messageParser import mParse
from regrid2 import Horizontal


class RegridKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("regrid", "Regridder", "Regrids the inputs using UVCDAT", parallelize=True ) )
        self._debug = False

    def getGrid(self, gridFilePath, latInterval = None, lonInterval = None ):
        import cdms2
        gridfile = cdms2.open(gridFilePath)
        baseGrid = gridfile.grids.values()[0]
        if ( (latInterval == None) or (lonInterval == None)  ):  return baseGrid
        else: return baseGrid.subGrid( latInterval, lonInterval )

    def executeOperations(self, task, _inputs):
        cdms2.setAutoBounds(2)
        t0 = time.time()
        self.logger.info( " Execute REGRID Task with metadata: " + str( task.metadata ) )
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
            else:
                gridSpec = task.metadata.get("gridSpec","")
                if not gridSpec: raise Exception( "Can't find crs spec in regrid kernel: " + str(crsToks))
                toGrid = getGrid( gridSpec )

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

                result_var = variable.regrid( toGrid, regridTool="esmf", regridMethod="linear" )
                self.logger.info( " >> Gridded Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) ) )
                results.append( self.createResult( result_var, _input, task ) )
        t1 = time.time()
        self.logger.info(" @RRR@ Completed regrid operation for input variables: {0} in time {1}".format( str(_inputs.keys), (t1 - t0)))
        return results

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting by default", parallelize=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        variable = _input.getVariable()
        axis = task.metadata.get("axis","xy")
#        weights = task.metadata.get( "weights", "" ).split(",")
#        if weights == [""]: weights = [ ("generate" if( axis == 'y' ) else "equal") for axis in axes ]
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axis, weights=weights, action=action, returned=returned )
        rv = self.createResult( result_var, _input, task )
        return rv

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
