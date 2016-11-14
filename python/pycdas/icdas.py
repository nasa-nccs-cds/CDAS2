from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from py4j.java_gateway import  DEFAULT_ADDRESS
import sys

class ICDAS(object):

    def sayHello(self, int_value, string_value ):
        print(int_value, string_value)
        return "Said hello to {0}".format(string_value)

    def sendData(self, trans_array ):
        try:
            rvs = [  "Shape value: {0}".format( trans_array.getShape()[1] ) , "Data value: {0}".format( trans_array.getData()[1] ) ]
            return ", ".join( rvs )
        except Exception as err:
            return "Python Execution error: {0}".format(err)

    class Java:
        implements = ["nasa.nccs.cdas.pyapi.ICDAS"]


icdas = ICDAS()
java_port = int(sys.argv[1])
python_port = int(sys.argv[2])
java_parms = JavaParameters(DEFAULT_ADDRESS,java_port)
python_parms = PythonParameters(DEFAULT_ADDRESS,python_port)

gateway = ClientServer( java_parameters=java_parms, python_parameters=python_parms, python_server_entry_point=icdas)
