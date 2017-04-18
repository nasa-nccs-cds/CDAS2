from pycdas.kernels.Kernel import CDMSKernel, Kernel, KernelSpec
from pycdas.cdasArray import cdmsArray
import cdms2, time, os, cdutil
from pycdas.messageParser import mParse
import numpy as np

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting by default", handlesInput=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        self.logger.info( "Executing AverageKernel, input metadata = " + str(_input.metadata) )
        dset_address = _input.metadata.get("uri", _input.metadata.get("dataPath") )
        vname = _input.metadata.get("name")
        dset = cdms2.open( dset_address )
        selector = _input.getSelector( dset[vname] )
        self.logger.info( "exec *EXT* AverageKernel, selector: " + str( selector ) )
        variable = dset( vname, **selector )
        axes = task.metadata.get("axis","xy")
#        weights = task.metadata.get( "weights", "" ).split(",")
#        if weights == [""]: weights = [ ("generate" if( axis == 'y' ) else "equal") for axis in axes ]
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axes, weights=weights, action=action, returned=returned )
        self.logger.info( "Computed result, input shape = " + str(variable.shape) + ", output shape = " + str(result_var.shape))
        rv = self.createResult( result_var, _input, task )
        self.logger.info( "Result data, shape = " + str(result_var.shape) + ", data = " + np.array_str( rv.array() )  )
        return rv

class ZonalAverageDemo(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("zaDemo", "ZonalAverageDemo", "Zonal average from -90 to 90", handlesInput=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        self.logger.info( "Executing AverageKernel, input metadata = " + str(_input.metadata) )
        dset_address = _input.metadata.get("uri", _input.metadata.get("dataPath") )
        vname = _input.metadata.get("name")
        dset = cdms2.open( dset_address )
        selector = _input.getSelector( dset[vname] )
        self.logger.info( "exec *EXT* AverageKernel, selector: " + str( selector ) )
        variable = dset( vname, **selector )
        axisIndex = variable.getAxisIndex( 'longitude' )

        cdutil.times.setTimeBoundsMonthly(variable)
        djfclimatology = cdutil.times.DJF.climatology(variable)
        zonalAve = cdutil.averager( djfclimatology, axis=axisIndex, weights='equal' )

        return self.createResult( zonalAve, _input, task )
