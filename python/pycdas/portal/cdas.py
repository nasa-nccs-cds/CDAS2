import zmq, traceback
from pycdas.kernels.Kernel import logger
from threading import Thread
from pycdas.cdasArray import npArray
from subprocess import Popen
import shlex

def bindSocket( socket, init_port ):
    test_port = init_port
    while( True ):
        try:
            socket.bind( "tcp://*:{0}".format(test_port) )
            break
        except Exception as err:
            test_port = test_port + 1
    return test_port

class CDASPortal:

    def __init__( self ):
        try:
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.PUSH)
            self.request_port = bindSocket( self.request_socket, 2335 )
            logger.info( "Started request socket on port: {0}".format( self.request_port ) )
            self.response_thread = ResponseThread(self.request_port + 1)
            self.response_thread.start()
        except Exception as err:
            err_msg =  "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
            logger.error(err_msg)
            print err_msg
            self.shutdown()

    def __del__(self): self.shutdown()

    def start_CDAS(self):
        self.application_thread = AppThread( self.request_port, self.response_thread.port )
        self.application_thread.start()

    def shutdown(self):
        logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.sendMessage("shutdown")
        try: self.request_socket.close()
        except Exception: pass
        self.application_thread.term()
        self.response_thread.term()

    def sendMessage( self, type, msgStrs = [""] ):
        logger.info( "Sending {1} Message: {0}\n".format( type, msgStrs )  )
        self.request_socket.send( "!".join( [type] + msgStrs ) )

    def join(self):
        self.response_thread.join()

class AppThread(Thread):
    def __init__(self, request_port, response_port):
        Thread.__init__(self)
        self._response_port = response_port
        self._request_port = request_port

    def run(self):
        cdas_startup = "cdas2 {0} {1} -J-Xmx32000M -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:MaxPermSize=800M".format( self._request_port, self._response_port )
        self.process = Popen( shlex.split( cdas_startup ) )
        logger.info( "Staring CDAS with command: {0}\n".format( cdas_startup ) )

    def term(self):
        self.process.terminate()

    def join(self):
        self.process.wait()

class ResponseThread(Thread):
    def __init__(self, init_port ):
        Thread.__init__(self)
        self.context = zmq.Context()
        self.cached_results = {}
        self.cached_arrays = {}
        self.socket = self.context.socket(zmq.PULL)
        self.port = bindSocket( self.socket, init_port )
        self.active = True
        self.setName('CDAS Response Thread')
        logger.info( "Started response socket on port: {0}".format( self.port ) )

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def run(self):
        while( self.active ):
            try:
                response = self.socket.recv()
                toks = response.split('|')
                type = toks[0]
                if type == "array":
                    header = toks[1]
                    data = self.request_socket.recv()
                    array = npArray.createInput(header,data)
                    self.cached_arrays[array.id] = array

                elif type == "result":
                    self.cached_results[ toks[1] ] = toks[2]
                    logger.info("Received result {0}: {1}".format(toks[1],toks[2]))

            except Exception as err:
                logger.error( "CDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ) )

    def term(self):
        self.active = False;
        try: self.socket.close()
        except Exception: pass
