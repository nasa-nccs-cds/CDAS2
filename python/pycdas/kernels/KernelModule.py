import OperationModule

class KernelModule(OperationModule):

    def __init__( self, name ):
        self.kernels = {}
        self.name = name
        self.build()


    def build(self):
        pass

    def executeTask( self, task, inputs ):
        kernel = self.kernels.get( task.op )
        kernel.executeTask( task, inputs )