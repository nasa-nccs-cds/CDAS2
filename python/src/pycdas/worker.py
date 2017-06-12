import os, traceback
import zmq, cdms2, logging
import numpy as np
from messageParser import mParse
from cdasArray import npArray, IO_DType
from kernels.OperationsManager import cdasOpManager
from task import Task

class Worker(object):

    def __init__( self, request_port, result_port ):
        self.cached_results = {}
        self.cached_inputs = {}
        self.logger = logging.getLogger("worker")
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
        self.request_socket.close()
        self.result_socket.close()

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def run(self):
        active = True
        try:
            while active:
                header = self.request_socket.recv()
                type = self.getMessageField(header,0)
                self.logger.info( "Received '{0}' message: {1}".format( type, header ) )
                if type == "array":
                    withData = int(self.getMessageField(header,5))
                    data = self.request_socket.recv() if withData else None
                    array = npArray.createInput(header,data)
                    self.cached_inputs[array.uid()] = array

                elif type == "task":
                    resultVars = self.processTask( Task(header) )
                    for resultVar in resultVars:
                        self.sendVariableData( resultVar )

                elif type == "util":
                    utype = self.getMessageField(header,1)
                    if utype == "capabilities":
                        capabilities = cdasOpManager.getCapabilitiesStr()
                        self.sendInfoMessage( capabilities )
                    elif utype == "quit":
                        self.logger.info(  " Quitting main thread. " )
                        active = False
                    elif utype == "exception":
                        raise Exception("Test Exception")

        except Exception as err:
            self.sendError( err )

    def sendVariableData( self, resultVar ):
        header = "|".join( [ "array-"+str(os.getpid()), resultVar.id,  mParse.ia2s(resultVar.origin), mParse.ia2s(resultVar.shape), mParse.m2s(resultVar.metadata) ] )
        self.logger.info( "Sending Result, header: {0}".format( header ) )
        self.result_socket.send( header )
        result_data = resultVar.toBytes( IO_DType )
        self.result_socket.send(result_data)

    def sendError( self, err ):
        msg = "Worker Error {0}: {1}".format(err, traceback.format_exc() )
        self.logger.error( msg  )
        header = "|".join( [ "error-"+str(os.getpid()), msg ] )
        self.result_socket.send( header )

    def sendInfoMessage( self, msg ):
        self.logger.info( "Worker Info: {0}\n".format( msg )  )
        header = "|".join( [ "info-"+str(os.getpid()), msg ] )
        self.result_socket.send( header )

    def processTask(self, task ):
        opModule = cdasOpManager.getModule( task )
        self.logger.info( " Processing task: {0}, op module: {1}".format( task, opModule.getName() ) )
        return opModule.executeTask( task, self.cached_inputs )

if __name__ == "__main__":
    request_port = mParse.getIntArg( 1, 8200 )
    result_port = mParse.getIntArg( 2, 8201 )
    worker = Worker( request_port, result_port )
    worker.run()
    worker.logger.info(  " ############################## EXITING WORKER ##############################"  )