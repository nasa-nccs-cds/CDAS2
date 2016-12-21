import logging, os
from abc import ABCMeta, abstractmethod

def _getLogger():
    logger = logging.getLogger('ICDAS')
    handler = logging.FileHandler( _getLogFile( os.getpid() ) )
    formatter = logging.Formatter('\t\t%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger

def _getLogFile( index ):
    log_file = os.path.expanduser('~/.cdas/pycdas-{0}.log'.format(index))
    try: os.remove(log_file)
    except Exception: pass
    return log_file

logger = _getLogger()

class KernelSpec:
    def __init__( self, name, title, description, inputs=[] ):
        self._name = name
        self._title = title
        self._description = description
        self._inputs = inputs

    def name(self): return self._name

    def __str__(self): return ";".join( [ self._name, self.getTitle(), self.getDescription(), ",".join(self._inputs) ] )

    def getDescription(self): return self._description.translate(None, ";,|!~^")
    def getTitle(self): return self._title.translate(None, ";,|!~^")

class Kernel:
    __metaclass__ = ABCMeta

    def __init__( self, spec ):
        self._spec = spec

    def name(self): return self._spec.name()

    def executeTask( self, task, inputs ):
        results = []
        for inputId in task.inputs:
            input = inputs.get( inputId )
            result = self.executeOperation( task, input )
            results.append( result )
        return results

    @abstractmethod
    def executeOperation( self, task, input ): pass

    def getCapabilities(self): return self._spec
    def getCapabilitiesStr(self): return str(self._spec)

    def getAxes( self, task ):
        axes = task.metadata.get("axes")
        if axes == None: return None
        else: return tuple( [ int(item) for item in axes ] )

class InputMode:
    __metaclass__ = ABCMeta

    def __init__( self, mode, spec ):
        self._spec = spec
        self._mode = mode

    @abstractmethod
    def execute(self): pass

if __name__ == "__main__":
    metadata = { "axes": "13" }
    axes = metadata.get("axes")
    if axes == None: print( "None")
    else: print( [ int(item) for item in axes ] )
