from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from py4j.java_gateway import  DEFAULT_ADDRESS
import logging, os, sys, traceback, array, time
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

            return "\n-------------------------------\n Got exec request part {0} \n-------------------------------\n ".format( self.partitionIndex )
        except Exception as err:
            return "\n-------------------------------\nPython Execution error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc())

    def execute(self, opId, contextStr, trans_arrays ):
        try:
            t0 = time.time()
            self.logger.info( "Executing Operation: {0}, context: {1}".format( opId, contextStr ) )
            inputs = [ self.getVariable( trans_array ) for trans_array in trans_arrays ]
            results = self.execOp( opId, getDict(contextStr), inputs )
            self.logger.info( "Completed Operation: {0} in time {1}".format( opId, (time.time()-t0) ) )

            return "\n-------------------------------\n Got exec request part {0} \n-------------------------------\n ".format( self.partitionIndex )
        except Exception as err:
            return "\n-------------------------------\nPython Execution error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc())

    def execOp( self, op, context, inputs ):
        t0 = time.time()
        opToks = op.split(".")
        module = opToks[0]
        opIdToks = opToks[1].split("-")
        opName = opIdToks[0]
        opId = opIdToks[1]
        crsToks = context.get("crs","gaussian:128").split(":")
        regridder = context.get("regridder","regrid2")
        crs = crsToks[0]
        resolution = int(crsToks[1]) if len(crsToks) > 1 else 128
        if opName.lower() == "regrid":
            if crs == "gaussian":
                t42 = cdms2.createGaussianGrid( resolution )
                rv = [ input.regrid( t42 , regridTool=regridder ) for input in inputs ]
                self.logger.info( " >> Regridded variables in time {0}".format( (time.time()-t0) ) )

    def getVariable( self, trans_array ):
        t0 = time.time()
        data = trans_array.data[:]
        origin  = trans_array.origin[:]
        shape  = trans_array.shape[:]
        t1 = time.time()
        nparray_flat = np.asarray( array.array( 'f', data) )
        t2 = time.time()
        nparray = nparray_flat.reshape( trans_array.shape )
        metadata = getDict( trans_array.metadata )
        gridFilePath = metadata["gridFile"]
        gridfile = cdms2.open( gridFilePath )
        name = metadata["name"]
        collection = metadata["collection"]
        dimensions = metadata["dimensions"].split(",")
        axes = [ gridfile.axes.get(dim) for dim in dimensions ]
        partition_axes = subsetAxes( axes, origin, shape )
        grid = gridfile.grids.values()[0]
        variable =  cdms2.createVariable( nparray, typecode=None, copy=0, savespace=0, mask=None, fill_value=trans_array.invalid, grid=grid, axes=partition_axes, attributes=metadata, id=collection+":"+name)
        t3 = time.time()
        self.logger.info( " >> Created Variable: {0} in times {1} {2} {3}".format( variable.id, (t1-t0), (t2-t1), (t3-t2) ) )
        self.logger.info( " >> Array Metadata: {0}".format( trans_array.metadata ) )
        self.logger.info( " >> Array Shape: [{0}]".format( ', '.join( map(str, trans_array.shape) ) ) )
        self.logger.info( " >> Array Origin: [{0}]".format( ', '.join( map(str, trans_array.origin) ) ) )
        self.logger.info( " >> Partition Axes: {0}".format(', '.join( [ axis.id for axis in partition_axes ] ) ) )
        return variable

    def getLogger( self, index ):
        logger = logging.getLogger('ICDAS')
        handler = logging.FileHandler( self.getLogFile(index) )
        formatter = logging.Formatter('\t\t%(asctime)s %(levelname)s %(message)s')
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
try:
    gateway = ClientServer( java_parameters=java_parms, python_parameters=python_parms, python_server_entry_point=icdas)

except Exception as err:
    icdas.logger.info("\n-------------------------------\nClientServer startup error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc()))
    sys.exit( -1 )

