
class KernelManager(object):

    def __init__( self ):
        self.operation_modules = {}
        self.build()


    def build(self):
        g = globals().copy()
        for name, obj in g.iteritems():
            if( isinstance(obj, types.ClassType) ):
                pass
