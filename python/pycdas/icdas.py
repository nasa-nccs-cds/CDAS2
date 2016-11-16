from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from py4j.java_gateway import  DEFAULT_ADDRESS
import logging, os, sys
import cdms2
import numpy as np

def getIntArg( index, default ): return sys.argv[index] if index < len( sys.argv ) else default


class ICDAS(object):

    def __init__(self, part_index ):
        self.logger = self.getLogger(part_index)
        self.partitionIndex = part_index;

    def sayHello(self, int_value, string_value ):
        print(int_value, string_value)
        return "Said hello to {0}".format(string_value)

    def sendData( self, trans_arrays ):
        try:
            self.logger.info( "Inputs: " )
            for trans_array in trans_arrays:
                self.logger.info( " >> Array Metadata: {0}".format( trans_array.metadata ) )
                self.logger.info( " >> Array Shape: [{0}]".format( ', '.join( map(str, trans_array.shape) ) ) )
                self.logger.info( " >> Array Origin: [{0}]".format( ', '.join( map(str, trans_array.origin) ) ) )
                variable = self.getVariable( trans_array )
                self.logger.info( " >> Created Variable: {0}".format( variable.id ) )

            return " -- Got exec request part {0} -- ".format( self.partitionIndex )
        except Exception as err:
            return "Python Execution error: {0}".format(err)

    def execute(self, opId, context, trans_arrays ):
        try:
            self.logger.info( "Executing Operation: {0}".format( opId ) )
            self.logger.info( "Context: {0}".format( context ) )
            self.logger.info( "Inputs: " )
            for trans_array in trans_arrays:
                self.logger.info( " >> Array Metadata: {0}".format( trans_array.metadata ) )
                self.logger.info( " >> Array Shape: [{0}]".format( ', '.join( map(str, trans_array.shape) ) ) )
                self.logger.info( " >> Array Origin: [{0}]".format( ', '.join( map(str, trans_array.origin) ) ) )
                variable = self.getVariable( trans_array )
                self.logger.info( " >> Created Variable: {0}".format( variable.id ) )

            return " -- Got exec request part {0} -- ".format( self.partitionIndex )
        except Exception as err:
            return "Python Execution error: {0}".format(err)

    def getVariable( self, trans_array ):
        array = np.ndarray( trans_array.shape, dtype=float, order='C', buffer=trans_array.data )
        return cdms2.createVariable( array, typecode=None, copy=0, savespace=0, mask=None, fill_value=trans_array.invalid, grid=None, axes=None,attributes=None, id=None)

    def getLogger( self, index ):
        logger = logging.getLogger('ICDAS')
        handler = logging.FileHandler( self.getLogFile(index) )
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger

    def getLogFile( self, index ):
        log_file = os.path.expanduser('~/.cdas/pycdas-{0}.log'.format(index))
        try: os.remove(log_file)
        except Exception: pass
        return log_file

    class Java:
        implements = ["nasa.nccs.cdas.pyapi.ICDAS"]

part_index = getIntArg(1,0)
java_port = getIntArg(2,8201)
python_port = getIntArg(3,8200)
java_parms = JavaParameters(DEFAULT_ADDRESS,java_port,True,True,True)
python_parms = PythonParameters(DEFAULT_ADDRESS,python_port)
icdas = ICDAS(part_index)
icdas.logger.info( " Running ICDAS-{0} on ports: {1} {2}".format( part_index, java_port, python_port ) )
gateway = ClientServer( java_parameters=java_parms, python_parameters=python_parms, python_server_entry_point=icdas)
