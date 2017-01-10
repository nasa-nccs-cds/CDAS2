from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from py4j.java_gateway import  DEFAULT_ADDRESS
import logging, os, sys, traceback, array, time, socket
import cdms2
import numpy as np

def getIntArg( index, default ): return int(sys.argv[index]) if index < len( sys.argv ) else default

def getDict( mdataStr ):
    metadata = {}
    for item in mdataStr.split(";"):
        toks = item.split(":")
        if len(toks) == 2:
            metadata[ toks[0] ] = toks[1]
    return metadata

def subsetAxes( axes, origin, shape ):
    subAxes = []
    for index in range( len(shape) ):
        start = origin[index]
        length = shape[index]
        subAxes.append( axes[index].subAxis( start, start + length) )
    return subAxes

class ICDAS(object):

    def __init__(self, part_index, data_port ):
        self.logger = self.getLogger(part_index)
        self.partitionIndex = part_index;
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data_socket.connect( ('127.0.0.1', data_port) )
        self.logger.info( "Connected data socket on port: {0}".format( data_port ) )
        self.cached_results = {}

    def __del__(self):
        self.logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.data_socket.close()

    def sayHello(self, int_value, string_value ):
        print(int_value, string_value)
        return "Said hello to {0}".format(string_value)

    def execute(self, opId, contextStr, trans_arrays ):
        try:
            t0 = time.time()
            self.logger.info( "Executing Operation: {0}, context: {1}".format( opId, contextStr ) )
            inputs = []
            for trans_array in trans_arrays:
                self.logger.info( "Receiving {0} bytes of data for trans_array: {1}".format( trans_array.nbytes, trans_array.metadata ) )
                byte_data = self.data_socket.recv( trans_array.nbytes )
                array_data = np.frombuffer( byte_data, dtype='f' ).reshape( trans_array.shape )
                input = self.getVariable( trans_array, array_data )
                inputs.append( input )
            results = self.execOp( opId, getDict(contextStr), inputs )
            response = self.saveResults( opId, inputs, results  )
            self.logger.info( "Completed Operation: {0} in time {1}".format( opId, (time.time()-t0) ) )
            return response
        except Exception as err:
            return "\n-------------------------------\nPython Execution error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc())

    def saveResults(self, opId, inputs, results ):
        responses = []
        for i in range( len(results) ):
            result = results[i]
            input = inputs[i]
            resultId = "-".join( [ input.id, opId ] )
            mdatafile = self.saveMetadata( resultId, result )
            shape_str = '-'.join( map(str, result.shape) )
            self.cached_results[resultId] = result
            responses.append( ",".join( [resultId,mdatafile,str(result.size*4),shape_str] ) )
        return ';'.join( responses )

    def getData( self, resultId ):
        try:
            result_variable = self.cached_results[resultId]
            result_data = result_variable.data.tobytes()
            self.logger.info( "Sending Result data,  nbytes: {0}".format( str(len(result_data) ) ) )
            self.data_socket.sendall( result_data )
        except KeyError:
            self.logger.info( "Can't find data element {0}, existing elements: [{1}]".format( resultId, ", ".join( result_data.keys() ) )  )

    def saveMetadata( self, resultId, variable  ):
        axes = variable.getAxisList()
        grid = variable.getGrid()
        outdir = os.path.dirname( variable.gridfile )
        outpath = os.path.join(outdir, resultId + ".nc" )
        newDataset = cdms2.createDataset( outpath )
        for axis in axes: newDataset.copyAxis(axis)
        newDataset.copyGrid(grid)
        newDataset.close()
        self.logger.info( "Saved metadata file: {0}".format( outpath ) )
        return outpath

    def execOp( self, op, context, inputs ):
        t0 = time.time()
        opToks = op.split(".")
        module = opToks[0]
        opIdToks = opToks[1].split("-")
        opName = opIdToks[0]
        opId = opIdToks[1]
        crsToks = context.get("crs","gaussian~128").split("~")
        regridder = context.get("regridder","regrid2")
        crs = crsToks[0]
        resolution = int(crsToks[1]) if len(crsToks) > 1 else 128
        if opName.lower() == "regrid":
            if crs == "gaussian":
                t42 = cdms2.createGaussianGrid( resolution )
                results = [ input.regrid( t42 , regridTool=regridder ) for input in inputs ]
                self.logger.info( " >> Regridded variables in time {0}, nresults = {1}".format( (time.time()-t0), len(results) ) )
                return results
        return []


    def getVariable( self, trans_array, nparray ):
        t0 = time.time()
        metadata = getDict( trans_array.metadata )
        gridFilePath = metadata["gridfile"]
        gridfile = cdms2.open( gridFilePath )
        name = metadata["name"]
        collection = metadata["collection"]
        dimensions = metadata["dimensions"].split(",")
        axes = [ gridfile.axes.get(dim) for dim in dimensions ]
        partition_axes = subsetAxes( axes, trans_array.origin, trans_array.shape )
        grid = gridfile.grids.values()[0]
        variable =  cdms2.createVariable( nparray, typecode=None, copy=0, savespace=0, mask=None, fill_value=trans_array.invalid, grid=grid, axes=partition_axes, attributes=metadata, id=collection+"-"+name)
        variable.createattribute( "gridfile", gridFilePath )
        t1 = time.time()
        self.logger.info( " >> Created Variable: {0} in time {1}".format( variable.id, (t1-t0) ) )
        self.logger.info( " >> Array Metadata: {0}".format( trans_array.metadata ) )
        self.logger.info( " >> Array Shape: [{0}]".format( ', '.join( map(str, trans_array.shape) ) ) )
        self.logger.info( " >> Array Origin: [{0}]".format( ', '.join( map(str, trans_array.origin) ) ) )
        self.logger.info( " >> Partition Axes: {0}".format(', '.join( [ axis.id for axis in partition_axes ] ) ) )
#        self.logger.info( " >> Data Stream Result: {0}".format( result.__class__.__name__ ) )
        return variable

    def getLogger( self, index ):
        logger = logging.getLogger('ICDAS')
        handler = logging.FileHandler( self.getLogFile(index) )
        formatter = logging.Formatter('\t\t %(message)s')
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
        implements = ["nasa.nccs.cdas.workers.ICDAS"]

part_index = getIntArg(1,0)
java_port = getIntArg(2,8201 )
python_port = getIntArg(3,8200)
data_port = getIntArg(4,8202)
java_parms = JavaParameters(DEFAULT_ADDRESS,java_port,True,True,True)
python_parms = PythonParameters(DEFAULT_ADDRESS,python_port)
icdas = ICDAS( part_index, data_port )

icdas.logger.info( " Running ICDAS-{0} on ports: {1} {2}".format( part_index, java_port, python_port ) )
try:
    gateway = ClientServer( java_parameters=java_parms, python_parameters=python_parms, python_server_entry_point=icdas)


except Exception as err:
    icdas.logger.info("\n-------------------------------\nClientServer startup error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc()))
    sys.exit( -1 )

