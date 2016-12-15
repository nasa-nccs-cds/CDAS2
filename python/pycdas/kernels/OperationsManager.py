from Modules import *

class OperationsManager(object):

    def __init__( self ):
        self.operation_modules = {}
        self.build()

    def build(self):
        g = globals().copy()
        for name, obj in g.iteritems():
            if( issubclass(obj.__class__, OperationModule ) ):
                print "Found Module: " + obj.name
                self.operation_modules[ obj.name ] = obj

    def getModule(self, task_header ):
        module_name = self.getModuleName(task_header)
        return self.operation_modules[ module_name ]

    def getModuleName(self,task_header ):
        header_toks = task_header.split('|')
        taskToks = header_toks[1].split('-')
        opToks = taskToks[0].split('.')
        return opToks[0]

cdasOpManager = OperationsManager()