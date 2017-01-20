from pycdas.kernels.Kernel import Kernel, KernelSpec
from pycdas.cdasArray import npArray
import os

class StdKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("std", "Standard Deviation", "Computes the standard deviation of the array elements along the given axes.", parallelize=False ) )  # Temporarily forcing some python kernels to run in serial mode

    def executeOperation( self, task, input ):
        result = input.array.std( self.getAxes(task), None, None, True )
        return npArray.createResult( task, input, result )

class MaxKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("max", "Maximum", "Computes the maximun of the array elements along the given axes.", reduceOp="max" ) )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.max( self.getAxes(task), None, None, True ) )

class MaxKernelSerial(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("maxSer", "Maximum (Serial)", "Computes the maximun of the array elements along the given axes without parallelization (for testing).", parallelize=False  ) )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.max( self.getAxes(task), None, None, True ) )

class MinKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("min", "Minimum", "Computes the minimun of the array elements along the given axes.", reduceOp="min" ) )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.min( self.getAxes(task), None, None, True ) )

class MeanKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Mean","Computes the mean of the array elements along the given axes.", parallelize=False ) )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.mean( self.getAxes(task), None, None, True ) )

class SumKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum","Computes the sum of the array elements along the given axes.", reduceOp="sum" ) )
    def executeOperation( self, task, input ):
        self.logger.info( " ------------------------------- SUM KERNEL: Operating on input '{0}', shape = {1}, origin = {2}".format( input.name, input.shape, input.origin ) )
        return npArray.createResult( task, input, input.array.sum( self.getAxes(task), None, None, True ) )

class PtpKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ptp", "Peak to Peak","Computes the peak to peak (maximum - minimum) value the along given axes.", parallelize=False ) )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.ptp( self.getAxes(task), None, None, True ) )

class VarKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("var", "Variance","Computes the variance of the array elements along the given axes.", parallelize=False ) )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.var( self.getAxes(task), None, None, True ) )

