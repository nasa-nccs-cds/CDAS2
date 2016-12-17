import os, traceback
import zmq, cdms2
import numpy as np
from messageParser import mParse
from cdasArray import CDArray
from kernels.OperationsManager import cdasOpManager
from kernels.Kernel import logger

class Worker(object):

    def __init__( self, request_port, result_port ):
        self.cached_results = {}
        self.cached_inputs = {}
        self.io_dtype = np.dtype( np.float32 ).newbyteorder('>')
        try:
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.PULL)
            self.request_socket.connect("tcp://localhost:" + str(request_port) )
            logger.info( "Connected request socket on port: {0}".format( request_port ) )
            self.result_socket = self.context.socket(zmq.PUSH)
            self.result_socket.connect("tcp://localhost:" + str(result_port) )
            logger.info( "Connected result socket on port: {0}".format( result_port ) )
        except Exception as err:
            logger.error( "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )


    def __del__(self): self.shutdown()

    def shutdown(self):
        logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.request_socket.close()
        self.result_socket.close()

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def run(self):
        logger.info( " Worker Running: listening for messages " )
        active = True
        while active:
            try:
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

                elif type == "util":
                    utype = self.getMessageField(header,1)
                    if utype == "capabilities":
                        capabilities = cdasOpManager.getCapabilitiesStr()
                        self.sendInfoMsg( capabilities )
                    elif utype == "quit":
                        logger.info(  " Quitting main thread. " )
                        active = False

            except Exception as err:
                logger.error( "Execution error %s:\n%s".format( err, traceback.format_exc() ) )
                self.sendError( err )

    def sendVariableData( self, resultVar ):
        header = "|".join( [ "array", resultVar.id, resultVar.origin, mParse.ia2s(resultVar.shape), mParse.m2s(resultVar.attributes) ] )
        logger.info( "Sending Result, header: {0}".format( header ) )
        self.result_socket.send( header )
        logger.info( " >> Result Data Sample: [ {0} ]".format( ', '.join(  [ str( resultVar.data.flat[i] ) for i in range(20,26) ] ) ) )
        result_data = resultVar.data.astype( self.io_dtype ).tobytes()
        logger.info( "Sending Result data,  nbytes: {0}".format( str(len(result_data) ) ) )
        self.result_socket.send(result_data)

    def sendError( self, err ):
        msg = "\n-------------------------------\nWorker Error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
        logger.error( msg  )
        header = "|".join( [ "error", msg ] )
        self.result_socket.send( header )

    def sendInfoMessage( self, msg ):
        msg = "Worker Info: {0}\n".format( msg )
        logger.info( msg  )
        header = "|".join( [ "info", msg ] )
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
        logger.info( "Saved grid file: {0}".format( outpath ) )
        return outpath

    def processTask(self, task_header ):
        opModule = cdasOpManager.getModule( task_header )
        return opModule.executeTask( task_header, self.cached_inputs )


request_port = mParse.getIntArg( 1, 8200 )
result_port = mParse.getIntArg( 2, 8201 )
worker = Worker( request_port, result_port )
worker.run()
worker.logger.info(  " ############################## EXITING WORKER ##############################"  )