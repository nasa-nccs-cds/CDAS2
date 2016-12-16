from Modules import *
from pycdas.kernels.Kernel import Kernel
from os import listdir
from os.path import isfile, join, os
_debug_ = True

class OperationsManager:

    def __init__( self ):
        self.operation_modules = []
        self.build()

    def build(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        internals_path = os.path.join( directory, "internal")
        allfiles = [ os.path.splitext(f) for f in listdir(internals_path) if ( isfile(join(internals_path, f)) ) ]
        modules = [ ftoks[0] for ftoks in allfiles if ( (ftoks[1] == ".py") and (ftoks[0] != "__init__") ) ]
        for module_name in modules:
            module_path = "pycdas.kernels.internal." + module_name
            module = __import__( module_path, globals(), locals(), ['*']  )
            for clsname in dir(module) :
                mod_cls = getattr( module, clsname)
                if( inspect.isclass(mod_cls) and issubclass( mod_cls, Kernel ) and (mod_cls.__module__ == module_path) ):
                    try:
                        mod_instance = mod_cls()
                        self.operation_modules.append(mod_instance)
                        if _debug_: print " ----------->> Adding Module: " + str( mod_instance )
                    except TypeError, err:
                        if _debug_: print "Skipping improperly structured op module: " + clsname + " -->> " + str(err)

    def getModule(self, task_header ):
        module_name = self.getModuleName(task_header)
        return self.operation_modules[ module_name ]

    def getModuleName(self,task_header ):
        header_toks = task_header.split('|')
        taskToks = header_toks[1].split('-')
        opToks = taskToks[0].split('.')
        return opToks[0]

    def getCapabilitiesStr(self):
        specs = [ opMod.getCapabilitiesStr()  for opMod in self.operation_modules ]
        return "|".join( specs )


cdasOpManager = OperationsManager()

if __name__ == "__main__":
    print( cdasOpManager.getCapabilitiesStr() )


