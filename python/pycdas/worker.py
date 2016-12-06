import sys, os, logging, traceback
import time, zmq, cdms2
import numpy as np
from pycdas.messageParser import mParse

def sa2s( strArray ): return ','.join( strArray )
def ia2s( intArray ): return ','.join( str(e) for e in intArray )
def null2s( val ):
    if val is not None: return val;
    else: return "";
def m2s( map  ): return ';'.join( key+":"+null2s(value) for key,value in map.iteritems() )
def s2b( s ):
    if s.lower() == "t": return True
    else: return False


class Worker(object):

    def __init__( self, request_port, result_port ):
        self.logger = self.getLogger( request_port )
        self.cached_results = {}
        self.cached_inputs = {}
        self.io_dtype = np.dtype( np.float32 ).newbyteorder('>')
        try:
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.PULL)
            self.request_socket.connect("tcp://localhost:" + str(request_port) )
            self.logger.info( "Connected request socket on port: {0}".format( request_port ) )
            self.result_socket = self.context.socket(zmq.PUSH)
            self.result_socket.connect("tcp://localhost:" + str(result_port) )
            self.logger.info( "Connected result socket on port: {0}".format( result_port ) )
        except Exception as err:
            self.logger.error( "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )


    def __del__(self): self.shutdown()

    def shutdown(self):
        self.logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.request_socket.close()
        self.result_socket.close()

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def run(self):
        self.logger.info(  " Running: listening for messages" )
        active = True
        while active:
            try:
                header = self.request_socket.recv()
                type = self.getMessageField(header,0)
                if type == "array":
                    data = self.request_socket.recv()
                    try:
                        self.loadVariable(header,data)
                    except Exception as err:
                        self.sendError( err )

                elif type == "task":
                    try:
                        resultVars = self.processTask( header )
                        for resultVar in resultVars:
                            self.sendVariableData( resultVar )
                    except Exception as err:
                        self.sendError( err )


                elif type == "quit":
                    self.logger.info(  " Quitting main thread. " )
                    active = False

            except Exception as err:
                self.sendError( err )

    def sendVariableData( self, resultVar ):
        header = "|".join( [ "array", resultVar.id, resultVar.origin, ia2s(resultVar.shape), m2s(resultVar.attributes) ] )
        self.logger.info( "Sending Result, header: {0}".format( header ) )
        self.result_socket.send( header )
        self.logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( resultVar.data.flat[i] ) for i in range(20,26) ] ) ) )
        result_data = resultVar.data.astype( self.io_dtype ).tobytes()
        self.logger.info( "Sending Result data,  nbytes: {0}".format( str(len(result_data) ) ) )
        self.result_socket.send(result_data)

    def sendError( self, err ):
        msg = "\n-------------------------------\nWorker Error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
        self.logger.error( msg  )
        header = "|".join( [ "error", msg ] )
        self.result_socket.send( header )

    def saveGridFile( self, resultId, variable  ):
        axes = variable.getAxisList()
        grid = variable.getGrid()
        outdir = os.path.dirname( variable.gridfile )
        outpath = os.path.join(outdir, resultId + ".nc" )
        newDataset = cdms2.createDataset( outpath )
        for axis in axes: newDataset.copyAxis(axis)
        newDataset.copyGrid(grid)
        newDataset.close()
        self.logger.info( "Saved grid file: {0}".format( outpath ) )
        return outpath

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

    def processTask(self, task_header ):
        import zmq, cdms2, numpy as np
        t0 = time.time()
        header_toks = task_header.split('|')
        taskToks = header_toks[1].split('-')
        opToks = taskToks[0].split('.')
        module = opToks[0]
        op = opToks[1]
        rId = taskToks[1]
        inpitIds = header_toks[2].split(',')
        metadata = mParse.s2m( header_toks[3] )
        cacheReturn = [ s2b(s) for s in metadata.get( "cacheReturn", "ft" ) ]
        inputs = [ self.cached_inputs.get( inputId ) for inputId in inpitIds ]

        if( op == "regrid" ):
            crsToks = metadata.get("crs","gaussian~128").split("~")
            regridder = metadata.get("regridder","regrid2")
            crs = crsToks[0]
            resolution = int(crsToks[1]) if len(crsToks) > 1 else 128
            if crs == "gaussian":
                results = []
                t42 = cdms2.createGaussianGrid( 128 ) # resolution )
                for input in inputs:
                    self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( input.data.flat[i] ) for i in range(20,90) ] ) ) )
                    result = input.regrid( t42 ) # , regridTool=regridder )
                    result.id = result.id  + "-" + rId
                    self.logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( result.data.flat[i] ) for i in range(20,90) ] ) ) )
                    gridFilePath = self.saveGridFile( result.id, result )
                    result.createattribute( "gridfile", gridFilePath )
                    result.createattribute( "origin", input.attributes[ "origin"] )
                    if cacheReturn[0]: self.cached_results[ result.id ] = result
                    if cacheReturn[1]: results.append( result )
                self.logger.info( " >> Regridded variables in time {0}, nresults = {1}".format( (time.time()-t0), len(results) ) )
                return results

    def loadVariable( self, header, data ):
        import zmq, cdms2, numpy as np
        t0 = time.time()
        self.logger.info(  " *** Got data, nbytes = : " + str(len(data)) )
        header_toks = header.split('|')
        vid = header_toks[1]
        origin = mParse.s2ia( header_toks[2] )
        shape = mParse.s2ia( header_toks[3] )
        metadata = mParse.s2m( header_toks[4] )
        gridFilePath = metadata["gridfile"]
        gridfile = cdms2.open( gridFilePath )
        name = metadata["name"]
        var = gridfile[name]
        collection = metadata["collection"]
        dimensions = metadata["dimensions"].split(",")
        axes = [ gridfile.axes.get(dim) for dim in dimensions ]
        grid = gridfile.grids.values()[0]
        nparray = np.frombuffer( data, dtype=self.io_dtype ).reshape( shape ).astype( np.float32 )

        self.logger.info( " >> Array Metadata: {0}".format( metadata ) )
        self.logger.info( " >> Array Shape: [{0}]".format( ', '.join( map(str, shape) ) ) )
        self.logger.info( " >> Array Dimensions: [{0}]".format( ', '.join( map(str, dimensions) ) ) )
        self.logger.info( " >> Array Origin: [{0}]".format( ', '.join( map(str, origin) ) ) )

        partition_axes = self.subsetAxes( axes, origin, shape )
        variable =  cdms2.createVariable( nparray, typecode=None, copy=0, savespace=0, mask=None, fill_value=var.getMissing(), grid=grid, axes=partition_axes, attributes=metadata, id=collection+"-"+name)
        variable.createattribute( "gridfile", gridFilePath )
        variable.createattribute( "origin", ia2s(origin) )
        t1 = time.time()
        self.logger.info( " >> Created Variable: {0} ({1} in time {2}".format( variable.id, name,  (t1-t0) ) )
        self.cached_inputs[vid] = variable

    def subsetAxes( self, axes, origin, shape ):
        subAxes = []
        try:
            for index in range( len(axes) ):
                start = origin[index]
                length = shape[index]
                axis = axes[index]
                subAxes.append( axis.subAxis( start, start + length ) )
        except Exception as err:
            self.logger.info( "\n-------------------------------\nError subsetting Axes: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )
            self.logger
        return subAxes

request_port = mParse.getIntArg( 1, 8200 )
result_port = mParse.getIntArg( 2, 8201 )
worker = Worker( request_port, result_port )
worker.run()
worker.logger.info(  " ############################## EXITING WORKER ##############################"  )