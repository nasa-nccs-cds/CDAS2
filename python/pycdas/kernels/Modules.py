import sys, inspect
from Kernel import Kernel

class OperationModule:

    def __init__( self, module ):
        self._module = module

    def getName(self): return self.__class__.__name__
    def getModule(self): return self._module

    def executeTask( self, task, inputs ):
        pass

    def getCapabilities(self):
        pass

    def serialize(self):
        pass


class KernelModule(OperationModule):

    def __init__( self, module ):
        self._kernels = {}
        OperationModule.__init__( self, module )
        self.build()

    def isLocal( self, obj ):
        str(obj).split('\'')[1].split('.')[0] == "__main__"

    def build(self):
        module = sys.modules[self.getModule()]
        for name, obj in module.__dict__.items():
            if inspect.isclass(obj) and issubclass( obj, Kernel ) and obj.__module__ == module.__name__:
                instance = obj()
                self._kernels[instance.name()] = instance

    def executeTask( self, task, inputs ):
        try:
            kernel = self.kernels.get( task.op )
            kernel.executeTask(task, inputs)
        except Exception, err:
            print err

    def getCapabilities(self): return [ kernel.getCapabilities() for kernel in self._kernels.values() ]
    def getCapabilitiesStr(self): return "~".join([ kernel.getCapabilitiesStr() for kernel in self._kernels.values() ])

    def serialize(self): return "!".join( [self.__class__.__name__, self.getCapabilitiesStr() ] )


