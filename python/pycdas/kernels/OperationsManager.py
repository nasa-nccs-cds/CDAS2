from Modules import *
import importlib, types
from pycdas.kernels.internal.pycdasModule import *

# def list_modules(module_name):
#     try:
#         module = __import__(module_name, globals(), locals(), [module_name.split('.')[-1]])
#     except ImportError:
#         return
#     print(module_name)
#     for name in dir(module):
#         if type(getattr(module, name)) == types.ModuleType:
#             list_modules('.'.join([module_name, name]))
#
# kernelModuleList = [ "pycdas.kernels.internal" ]

# for kernelModule in kernelModuleList:
#     list_modules(kernelModule)
#     # kmods = kernelModule.split(".")
#     # parent = ".".join( kmods[0:len(kmods)-1] )
#     # imp_mod = importlib.import_module( kernelModule, package=parent )
#     # for key, obj in imp_mod.__dict__.iteritems():
#     #     print " ====>>> " + key


class OperationsManager:

    def __init__( self ):
        self.operation_modules = []
        self.build()

    def build(self):
        g = globals().copy()
        for name, mod_cls in g.iteritems():
            if( inspect.isclass(mod_cls) ):
                if( issubclass( mod_cls, OperationModule ) ):
                    try:
                        mod_instance = mod_cls()
                        self.operation_modules.append(mod_instance)
                        print " ----------->> Adding Module: " + str( mod_instance )
                    except TypeError, err:
                        print "Skipping improperly structured op module: " + name + " -->> " + str(err)

    def getModule(self, task_header ):
        module_name = self.getModuleName(task_header)
        return self.operation_modules[ module_name ]

    def getModuleName(self,task_header ):
        header_toks = task_header.split('|')
        taskToks = header_toks[1].split('-')
        opToks = taskToks[0].split('.')
        return opToks[0]

    def getCapabilitiesStr(self):
        specs = [ opMod.serialize()  for opMod in self.operation_modules ]
        return "|".join( specs )


cdasOpManager = OperationsManager()

if __name__ == "__main__":
    print( cdasOpManager.getCapabilitiesStr() )