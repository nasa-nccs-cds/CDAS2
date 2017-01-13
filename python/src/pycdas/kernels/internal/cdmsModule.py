from pycdas.kernels.Kernel import Kernel, KernelSpec
from pycdas.cdasArray import npArray
import os
import cdms2, time
from pycdas.messageParser import mParse

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
            results = []
            t42 = cdms2.createGaussianGrid( resolution )
            self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) ) )
            result = variable.regrid( t42, regridTool=regridder )
            result.id = result.id  + "-" + task.rId
            self.logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result.data.flat[i] ) for i in range(20,90) ] ) ) )
            gridFilePath = self.saveGridFile( result.id, result )
            result.createattribute( "gridfile", gridFilePath )
            result.createattribute( "origin", variable.attributes[ "origin"] )
            if cacheReturn[0]: self.cached_results[ result.id ] = result
            if cacheReturn[1]: results.append( result )
            self.logger.info( " >> Regridded variables in time {0}, nresults = {1}".format( (time.time()-t0), len(results) ) )
            return results


