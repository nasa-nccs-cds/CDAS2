from abc import ABCMeta, abstractmethod
import logging

class KernelSpec:
    def __init__( self, name, title, description, **kwargs ):
        self._name = name
        self._title = title
        self._description = description
        self._options = kwargs

    def name(self): return self._name

    def __str__(self): return ";".join( [ self._name, self.getTitle(), self.getDescription(), str(self._options) ] )

    def getDescription(self): return self._description.translate(None, ";,|!~^")
    def getTitle(self): return self._title.translate(None, ";,|!~^")

class Kernel:
    __metaclass__ = ABCMeta

    def __init__( self, spec ):
        self.logger = logging.getLogger("worker")
        self._spec = spec

    def name(self): return self._spec.name()

    def executeTask( self, task, inputs ):
        results = []
        for inputId in task.inputs:
            input = inputs.get( inputId )
            if( input != None ):
                result = self.executeOperation( task, input )
                results.append( result )
            else:
                raise Exception( "ExecuteTask ERROR: required input {0} not available in task inputs: {1}".format( task.inputs, inputs.keys() ))
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
    metadata = { "axes": "13", "index": 0 }

    print( str(metadata) )
