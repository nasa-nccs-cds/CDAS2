from pycdas.kernels.Kernel import Kernel, InputMode, KernelSpec, logger
from pycdas.cdasArray import CDArray
import cdms2, time
from pycdas.messageParser import mParse

class RegridKernel(Kernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("regrid", "Regridder", "Regrids the inputs using UVCDAT",["input"]) )

    def executeTask( self, task, inputs ):
        t0 = time.time()
        cacheReturn = [ mParse.s2b(s) for s in task.metadata.get( "cacheReturn", "ft" ) ]
        inputs = [ inputs.get( inputId ).getVariable() for inputId in task.inputs ]
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
                result.id = result.id  + "-" + task.rId
                self.logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result.data.flat[i] ) for i in range(20,90) ] ) ) )
                gridFilePath = self.saveGridFile( result.id, result )
                result.createattribute( "gridfile", gridFilePath )
                result.createattribute( "origin", input.attributes[ "origin"] )
                if cacheReturn[0]: self.cached_results[ result.id ] = result
                if cacheReturn[1]: results.append( result )
            self.logger.info( " >> Regridded variables in time {0}, nresults = {1}".format( (time.time()-t0), len(results) ) )
            return results

class CDMSInput(InputMode):

    def __init__(self, spec ):
        InputMode.__init__( self, "url", spec )


    def execute(self):
        f=cdms2.open(url,'r')

