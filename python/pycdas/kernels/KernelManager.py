from Modules import *

class KernelManager(object):

    def __init__( self ):
        self.operation_modules = {}
        self.build()

    def build(self):
        g = globals().copy()
        for name, obj in g.iteritems():
            if( issubclass(obj.__class__, OperationModule ) ):
                print "Found Module: " + obj.name
                self.operation_modules[ obj.name ] = obj

    def getModule(self, name ):
        return self.operation_modules[ name ]

if __name__ == "__main__":
    kernel_module = KernelModule("test_kernel")
    km = KernelManager()