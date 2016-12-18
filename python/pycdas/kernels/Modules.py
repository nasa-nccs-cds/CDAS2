import sys, inspect
from Kernel import Kernel

class OperationModule:

    def __init__( self, name ):
        self._name = name

    def getName(self): return self._name

    def executeTask( self, task, inputs ):
        pass

    def getCapabilities(self):
        pass

    def getCapabilitiesStr(self):
        pass

    def serialize(self): return "!".join( [self._name, "python", self.getCapabilitiesStr() ] )


class KernelModule(OperationModule):

    def __init__( self, name, kernels ):
        self._kernels = {}
        for kernel in kernels: self._kernels[ kernel.name() ] = kernel
        OperationModule.__init__( self, name )

    def isLocal( self, obj ):
        str(obj).split('\'')[1].split('.')[0] == "__main__"

    def executeTask( self, task, inputs ):
        try:
            kernel = self.kernels.get( task.op )
            kernel.executeTask(task, inputs)
        except Exception, err:
            print err

    def getCapabilities(self): return [ kernel.getCapabilities() for kernel in self._kernels.values() ]
    def getCapabilitiesStr(self): return "~".join([ kernel.getCapabilitiesStr() for kernel in self._kernels.values() ])



