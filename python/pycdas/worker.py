import sys, os, logging, traceback
import time, zmq, cdms2
import numpy as np
from messageParser import mParse
from cdasArray import CDArray

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
        try:
            self.logger.info(  " XX Running: listening for messages" )
            active = True
            while active:
                header = self.request_socket.recv()
                type = self.getMessageField(header,0)
                if type == "array":
                    data = self.request_socket.recv()
                    array = CDArray(header,data)
                    self.cached_inputs[array.id] = array

                elif type == "task":
                    resultVars = self.processTask( header )
                    for resultVar in resultVars:
                        self.sendVariableData( resultVar )

                elif type == "quit":
                    self.logger.info(  " Quitting main thread. " )
                    active = False

        except Exception as err:
            self.logger.error( "Execution error %s:\n%s".format( err, traceback.format_exc() ) )
            self.sendError( err )

    def sendVariableData( self, resultVar ):
        header = "|".join( [ "array", resultVar.id, resultVar.origin, mParse.ia2s(resultVar.shape), mParse.m2s(resultVar.attributes) ] )
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
        inputIds = header_toks[2].split(',')
        metadata = mParse.s2m( header_toks[3] )
        cacheReturn = [ mParse.s2b(s) for s in metadata.get( "cacheReturn", "ft" ) ]
        inputs = [ ]
        results = []
        for inputId in inputIds:
            try:
                input = self.cached_inputs[ inputId ]
                inputs.append( input )
            except:  "Missing input to task {0}: {1}".format( op, inputId )


        if( op == "regrid" ):
            crsToks = metadata.get("crs","gaussian~128").split("~")
            regridder = metadata.get("regridder","regrid2")
            crs = crsToks[0]
            resolution = int(crsToks[1]) if len(crsToks) > 1 else 128
            if crs == "gaussian":

                t42 = cdms2.createGaussianGrid( resolution )
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

request_port = mParse.getIntArg( 1, 8200 )
result_port = mParse.getIntArg( 2, 8201 )
worker = Worker( request_port, result_port )
worker.run()
worker.logger.info(  " ############################## EXITING WORKER ##############################"  )