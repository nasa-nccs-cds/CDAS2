from Modules import *
from pycdas.kernels.Kernel import Kernel, logger
import pycdas
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
            kernels = []
            for clsname in dir(module):
                mod_cls = getattr( module, clsname)
                if( inspect.isclass(mod_cls) and (mod_cls.__module__ == module_path) ):
                    try:
                        if issubclass( mod_cls, Kernel ):
                            kernel_instance = mod_cls();
                            kernels.append( kernel_instance )
                            logger.debug(  " ----------->> Adding Kernel Class: " + str( clsname ) )
                    except TypeError, err:
                        logger.debug( "Skipping improperly structured class: " + clsname + " -->> " + str(err) )
            if len(kernels) > 0:
                self.operation_modules.append( KernelModule( module_name, kernels ) )
                logger.debug(  " ----------->> Adding Module: " + str( module_name ) )
    def getModule(self, task_header ):
        module_name = self.getModuleName(task_header)
        return self.operation_modules[ module_name ]

    def getModuleName(self,task_header ):
        header_toks = task_header.split('|')
        taskToks = header_toks[1].split('-')
        opToks = taskToks[0].split('.')
        return opToks[0]

    def getCapabilitiesStr(self):
        specs = [ opMod.serialize() for opMod in self.operation_modules ]
        return "|".join( specs )

cdasOpManager = OperationsManager()

if __name__ == "__main__":
    print( cdasOpManager.getCapabilitiesStr() )


