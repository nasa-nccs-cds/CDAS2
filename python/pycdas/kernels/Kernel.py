
class KernelSpec:
    def __init__( self, name, description, inputs=[] ):
        self._name = name
        self._description = description
        self._inputs = inputs

    def name(self): return self._name

    def __str__(self): return ";".join( [ self._name, self.getDescription(), ",".join(self._inputs) ] )

    def getDescription(self): return self._description.translate(None, ";,|!~^")

class Kernel:

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

    def executeOperation( self, task, input ):
        pass

    def getCapabilities(self): return self._spec
    def getCapabilitiesStr(self): return str(self._spec)

    def getAxes( self, task ):
        axes = task.metadata.get("axes")
        if axes == None: return None
        else: return [ int(item) for item in axes ]


if __name__ == "__main__":
    metadata = { "axes": "13" }
    axes = metadata.get("axes")
    if axes == None: print( "None")
    else: print( [ int(item) for item in axes ] )
