from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from py4j.java_collections import JavaArray

class ICDAS(object):

    def sayHello(self, int_value, string_value ):
        print(int_value, string_value)
        return "Said hello to {0}".format(string_value)

    def sendArray(self, data, shape ):
        print( "Int Array value: {0}".format( shape[1] ) )
        return "Got array"

    class Java:
        implements = ["nasa.nccs.cdas.pyapi.ICDAS"]


icdas = ICDAS()

gateway = ClientServer(
    java_parameters=JavaParameters(),
    python_parameters=PythonParameters(),
    python_server_entry_point=icdas)
