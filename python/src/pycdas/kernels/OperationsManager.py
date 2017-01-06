from Modules import *
from pycdas.kernels.Kernel import Kernel
import logging
from os import listdir
from os.path import isfile, join, os

class OperationsManager:

    def __init__( self ):
        self.logger =  logging.getLogger("worker")
        self.operation_modules = {}
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
                            self.logger.debug(  " ----------->> Adding Kernel Class: " + str( clsname ) )
                    except TypeError, err:
                        self.logger.debug( "Skipping improperly structured class: " + clsname + " -->> " + str(err) )
            if len(kernels) > 0:
                self.operation_modules["python."+module_name] = KernelModule( module_name, kernels )
                self.logger.debug(  " ----------->> Adding Module: " + str( module_name ) )

    def getModule(self, task ):
        return self.operation_modules[ task.module ]

    def getCapabilitiesStr(self):
        specs = [ opMod.serialize() for opMod in self.operation_modules.itervalues() ]
        return "|".join( specs )

cdasOpManager = OperationsManager()

if __name__ == "__main__":
    print( cdasOpManager.getCapabilitiesStr() )


