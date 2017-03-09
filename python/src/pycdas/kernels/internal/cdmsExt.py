from pycdas.kernels.Kernel import CDMSKernel, Kernel, KernelSpec
from pycdas.cdasArray import cdmsArray
import cdms2, time, os, cdutil
from pycdas.messageParser import mParse

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting by default", handlesInput=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        dset_address = _input.metadata.get("uri", _input.metadata.get("dataPath") )
        vname = _input.metadata.get("name")
        dset = cdms2.open( dset_address )
        selector = _input.getSelector( dset[vname] )
        self.logger.info( "exec *EXT* AverageKernel, selector: " + str( selector ) )
        variable = dset( vname, **selector )
        axis = task.metadata.get("axis","xy")
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axis, weights=weights, action=action, returned=returned )
        return self.createResult( result_var, _input, task )
