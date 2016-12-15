from pycdas.kernels.Kernel import Kernel
from pycdas.kernels.Modules import KernelModule
import cdms2, numpy as np, time
import sys, inspect
from pycdas.messageParser import mParse

class PycdasModule(KernelModule):
    def __init__( self ): KernelModule.__init__( self, sys.modules[__name__]  ) # Always use this boilerplate to initialize the KernelModule

class RegridKernel(Kernel):

    def __init__( self ):
        Kernel.__init__(self,"regrid")

    def executeTask( self, task, inputs ):

        t0 = time.time()
        cacheReturn = [ mParse.s2b(s) for s in task.metadata.get( "cacheReturn", "ft" ) ]
        inputs = [ inputs.get( inputId ) for inputId in task.inputs ]
        crsToks = task.metadata.get("crs","gaussian~128").split("~")
        regridder = task.metadata.get("regridder","regrid2")
        crs = crsToks[0]
        resolution = int(crsToks[1]) if len(crsToks) > 1 else 128
        if crs == "gaussian":
            results = []
            t42 = cdms2.createGaussianGrid( resolution )
            for input in inputs:
                self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( input.data.flat[i] ) for i in range(20,90) ] ) ) )
                result = input.regrid( t42, regridTool=regridder )
                result.id = result.id  + "-" + rId
                self.logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result.data.flat[i] ) for i in range(20,90) ] ) ) )
                gridFilePath = self.saveGridFile( result.id, result )
                result.createattribute( "gridfile", gridFilePath )
                result.createattribute( "origin", input.attributes[ "origin"] )
                if cacheReturn[0]: self.cached_results[ result.id ] = result
                if cacheReturn[1]: results.append( result )
            self.logger.info( " >> Regridded variables in time {0}, nresults = {1}".format( (time.time()-t0), len(results) ) )
            return results

if __name__ == "__main__":
    pycdasModule = PycdasModule()