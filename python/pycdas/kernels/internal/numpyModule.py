from pycdas.kernels.Kernel import Kernel, KernelSpec, logger
from pycdas.cdasArray import npArray

class StdKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("std", "Standard Deviation", "Computes the standard deviation of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        result = input.array.std( self.getAxes(task) )
        return npArray.createResult( task, input, result )

class MaxKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("max", "Maximum", "Computes the maximun of the array elements along the given axes.") )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.max( self.getAxes(task) ) )

class MinKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("min", "Minimum", "Computes the minimun of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.min( self.getAxes(task) ) )

class MeanKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Mean","Computes the mean of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.mean( self.getAxes(task) ) )

class SumKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum","Computes the sum of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.sum( self.getAxes(task) ) )

class PtpKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ptp", "Peak to Peak","Computes the peak to peak (maximum - minimum) value the along given axes.") )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.ptp( self.getAxes(task) ) )

class VarKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("var", "Variance","Computes the variance of the array elements along the given axes.") )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.var( self.getAxes(task) ) )

