import sys, inspect
from Kernel import Kernel

class OperationModule(object):

    def __init__( self, name ):
        self.name = name
        self.build()


    def build(self):
        pass

    def executeTask( self, task, inputs ):
        pass


class KernelModule(OperationModule):

    def __init__( self, name ):
        self.kernels = {}
        self.name = name
        self.build()

    def isLocal( self, obj ):
        str(obj).split('\'')[1].split('.')[0] == "__main__"

    def build(self):
        for name, obj in inspect.getmembers(self.name):
            if inspect.isclass(obj) and issubclass( obj, Kernel ) and (str(obj).split('\'')[1].split('.')[0] == "__main__"):
                instance = obj()
                self.kernels[instance.name] = instance
                print "Found kernel: " + instance.name

    def executeTask( self, task, inputs ):
        kernel = self.kernels.get( task.op )
