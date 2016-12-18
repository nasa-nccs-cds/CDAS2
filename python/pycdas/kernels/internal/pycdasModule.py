from pycdas.kernels.Kernel import Kernel, KernelSpec
import cdms2, os, time
from pycdas.messageParser import mParse

class StdKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("std", "Standard Deviation", "Computes the standard deviation of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return input.array.std( self.getAxes(task) )

class MaxKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("max", "Maximum", "Computes the maximun of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return input.array.max( self.getAxes(task) )

class MinKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("min", "Minimum", "Computes the minimun of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return input.array.min( self.getAxes(task) )

class MeanKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Mean","Computes the mean of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return input.array.mean( self.getAxes(task) )

class SumKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum","Computes the sum of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return input.array.sum( self.getAxes(task) )

class PtpKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ptp", "Peak to Peak","Computes the peak to peak (maximum - minimum) value the along given axes.") )
    def executeOperation( self, task, input ):
        return input.array.ptp( self.getAxes(task) )

class VarKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("var", "Variance","Computes the variance of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return input.array.var( self.getAxes(task) )

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