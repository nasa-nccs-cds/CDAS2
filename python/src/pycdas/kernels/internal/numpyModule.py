from pycdas.kernels.Kernel import Kernel, KernelSpec
from pycdas.cdasArray import npArray
import numpy as np
import time

class StdKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("std", "Standard Deviation", "Computes the standard deviation of the array elements along the given axes.", parallelize=False ) )  # Temporarily forcing some python kernels to run in serial mode

    def executeOperation( self, task, input ):
        result = input.array.std( axis=self.getAxes(task.metadata), keepdims=True )
        return npArray.createResult( task, input, result )

class MaxKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("max", "Maximum", "Computes the maximun of the array elements along the given axes.", reduceOp="max" ) )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.max( axis=self.getAxes(task.metadata), keepdims=True ) )

class MaxKernelSerial(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("maxSer", "Maximum (Serial)", "Computes the maximun of the array elements along the given axes without parallelization (for testing).", parallelize=False  ) )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.max( axis=self.getAxes(task.metadata), keepdims=True ) )

class MaxKernelCustom(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("maxCustRed", "Maximum (Serial)", "Computes the maximun of the array elements along the given axes without parallelization (for testing).", reduceOp="custom"  )  )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.max( axis=self.getAxes(task.metadata), keepdims=True ) )

    def reduce( self, input0, input1, task ):
        return npArray.createResult( task, input0, np.maximum( input0.array, input1.array ) )

class MinKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("min", "Minimum", "Computes the minimun of the array elements along the given axes.", reduceOp="min" ) )

    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.min( axis=self.getAxes(task.metadata), keepdims=True ) )


class SumKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum","Computes the sum of the array elements along the given axes.", reduceOp="sum" ) )
    def executeOperation( self, task, input ):
        self.logger.info( " ------------------------------- SUM KERNEL: Operating on input '{0}', shape = {1}, origin = {2}".format( input.name, input.shape, input.origin ) )
        return npArray.createResult( task, input, input.array.sum( axis=self.getAxes(task.metadata), keepdims=True ) )

class AverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the average of the array elements along the given axes.", reduceOp="sumw", postOp="normw", nOutputsPerInput=2 ) )

    def executeOperations(self, task, inputs):
        kernel_inputs = [inputs.get(inputId.split('-')[0]) for inputId in task.inputs]
        if None in kernel_inputs: raise Exception( "ExecuteTask ERROR: required input {0} not available in task inputs: {1}".format(task.inputs, inputs.keys()))
        results = []
        axes = self.getAxes(task.metadata)
        self.logger.info("\n\n Execute Operations, inputs: " + str( task.inputs ) + ", task metadata = " + str(task.metadata) + ", axes = " + str(axes) )
        for input in kernel_inputs:
            t0 = time.time()
            results.append( npArray.createResult( task, input,  input.array.sum( axis=axes,   keepdims=True ) ) )
            results.append( npArray.createAuxResult( task.rId + "_WEIGHTS_", input.origin, dict( input.metadata, **task.metadata ),  input.array.count(axis=self.getAxes(task.metadata), keepdims=True ) ) )
            t1 = time.time()
            self.logger.info( " ------------------------------- SUMW KERNEL: Operating on input '{0}', shape = {1}, origin = {2}, time = {3}".format( input.name, input.shape, input.origin, t1-t0 ))
        return results

class WeightedAverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("avew", "Weighted Average Kernel","Computes the weighted average of the array elements along the given axes.", reduceOp="avew", nOutputsPerInput=2 ) )

    def executeOperations(self, task, inputs):
        kernel_inputs = [inputs.get(inputId.split('-')[0]) for inputId in task.inputs]
        if None in kernel_inputs: raise Exception( "ExecuteTask ERROR: required input {0} not available in task inputs: {1}".format(task.inputs, inputs.keys()))
        results = []
        weightsType = task.metadata.get("weights","equal")
        axes=self.getAxes(task.metadata)
        self.logger.info("\n\n Execute Operations, inputs: " + str( task.inputs ) + ", task metadata = " + str(task.metadata) + ", axes = " + str(axes) )
        for input in kernel_inputs:
            weights = None
            result = input.array
            dimensions = input.metadata.get("dimensions","").split(',')
            axis_dims = [ dimensions[axis] for axis in axes ]
            t0 = time.time()
            for axis in axes:
                 result, weights = np.ma.average( result, axis=axis, weights=weights, returned=True )
            self.logger.info(" Input metadata: " + str( input.metadata ) + ", input shape = " + str(input.array.shape) + ", output shape = " + str(result.shape) )
            results.append( npArray.createResult( task, input, result ) )
            results.append( npArray.createAuxResult( task.rId + "_WEIGHTS_", input.origin, dict( input.metadata, **task.metadata ),  weights ) )
            t1 = time.time()
            self.logger.info( " ------------------------------- AVEW KERNEL: Operating on input '{0}', shape = {1}, origin = {2}, time = {3}".format( input.name, input.shape, input.origin, t1-t0 ))
        return results


class PtpKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ptp", "Peak to Peak","Computes the peak to peak (maximum - minimum) value the along given axes.", parallelize=False ) )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.ptp( axis=self.getAxes(task.metadata), keepdims=True ) )

class VarKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("var", "Variance","Computes the variance of the array elements along the given axes.", parallelize=False ) )
    def executeOperation( self, task, input ):
        return npArray.createResult( task, input, input.array.var( axis=self.getAxes(task.metadata), keepdims=True ) )

