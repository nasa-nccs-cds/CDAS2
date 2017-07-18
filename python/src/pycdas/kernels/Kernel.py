from abc import ABCMeta, abstractmethod
import logging, os, cdms2, time
from pycdas.messageParser import mParse
from pycdas.cdasArray import cdmsArray
import cdms2, time, os, cdutil

class KernelSpec:
    def __init__( self, name, title, description, **kwargs ):
        self._name = name
        self._title = title
        self._description = description
        self._options = kwargs

    def name(self): return self._name

    def __str__(self): return ";".join( [ self._name, self.getTitle(), self.getDescription(), str(self._options) ] )

    def getDescription(self): return self._description.translate(None, ";,|!~^")
    def getTitle(self): return self._title.translate(None, ";,|!~^")

class Kernel:
    __metaclass__ = ABCMeta

    def __init__( self, spec ):
        self.logger = logging.getLogger("worker")
        self._spec = spec
        self.cacheReturn = [ False, True ]

    def name(self): return self._spec.name()

    def executeTask( self, task, inputs ):
        t0 = time.time()
        self.cacheReturn = [ mParse.s2b(s) for s in task.metadata.get( "cacheReturn", "ft" ) ]
        results = self.executeOperations( task, inputs )
        self.logger.info( " >> Executed {0} operation in time {1}, #results = {2}".format( self._spec.name(), (time.time()-t0), len(results) ) )
        return results

    def executeReduceOp( self, task, inputs ):
        kernel_input_ids = [ inputId.split('-')[0] for inputId in task.inputs ]
        self.logger.info( " >> Executed Reduce operation on inputs {0}, available arrays: {1} ".format( str(kernel_input_ids), str(inputs.keys()) ) )
        return [ self.reduce( inputs[kernel_input_ids[0]], inputs[kernel_input_ids[1]], task ) ]

    def reduce( self, input0, input1, metadata, rId ): raise Exception( "Parallelizable kernel with undefined reduce method operating on T axis")

    def executeOperations( self, task, inputs ):
        self.logger.info( "\n\n Execute Operations, inputs: " + str(inputs) )
        kernel_inputs = [ inputs.get( inputId.split('-')[0] ) for inputId in task.inputs ]
        if None in kernel_inputs: raise Exception( "ExecuteTask ERROR: required input {0} not available in task inputs: {1}".format( task.inputs, inputs.keys() ))
        return [ self.executeOperation(task,input) for input in kernel_inputs ]

    def executeOperation( self, task, input ): raise Exception( "Attempt to execute Kernel with undefined executeOperation method")

    def getCapabilities(self): return self._spec
    def getCapabilitiesStr(self): return str(self._spec)

    def getAxes( self, metadata ):
        axes = metadata.get("axes")
        if axes == None: return None
        else: return tuple( [ int(item) for item in axes ] )

    def saveGridFile( self, resultId, variable  ):
        grid = variable.getGrid()
        outpath = None
        if( grid != None ):
            axes = variable.getAxisList()
            outdir = os.path.dirname( variable.gridfile )
            outpath = os.path.join(outdir, resultId + ".nc" )
            newDataset = cdms2.createDataset( outpath )
            for axis in axes:
                if axis.isTime:     pass
                else:               newDataset.copyAxis(axis)
            newDataset.copyGrid(grid)
            newDataset.close()
            self.logger.info( "Saved grid file: {0}".format( outpath ) )
        return outpath

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

class InputMode:
    __metaclass__ = ABCMeta

    def __init__( self, mode, spec ):
        self._spec = spec
        self._mode = mode

    @abstractmethod
    def execute(self): pass

if __name__ == "__main__":
    metadata = { "axes": "13", "index": 0 }

    print( str(metadata) )


if __name__ == "__main__":
    from cdms2 import timeslice
    dsetUri = "http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_195101-200512.nc"
    resolution = 128
    dset = cdms2.open(dsetUri)
    variable = dset["tas"](timeslice(0,1))
    ingrid = variable.getGrid()
    axes = variable.getAxisList()
    grid = variable.getGrid()
    outdir = os.path.dirname( variable.gridfile )
    outpath = os.path.expanduser('~/.cdas/debug_grid_file.nc' )
    newDataset = cdms2.createDataset( outpath )
    for axis in axes: newDataset.copyAxis(axis)
    newDataset.copyGrid(grid)



#     newDataset.close()
