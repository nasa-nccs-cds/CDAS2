import zmq, traceback
from pycdas.kernels.Kernel import logger
from threading import Thread
from pycdas.cdasArray import npArray
from subprocess import call
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
            logger.info( "Connected request socket on port: {0}".format( self.request_port ) )
            self.response_thread = ResponseThread(self.request_port + 1)
            self.response_thread.start()
        except Exception as err:
            logger.error( "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )

    def __del__(self): self.shutdown()

    def start_CDAS(self):
        cdas_startup = "cdas -pullport {0} -pushport {1} -J-Xmx32000M -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:MaxPermSize=800M".format( self.response_thread.port, self.request_port )
        call( shlex.split( cdas_startup ) )

    def shutdown(self):
        logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.sendMessage("shutdown")
        self.request_socket.close()
        self.result_socket.close()

    def sendMessage( self, type, msg = "" ):
        logger.info( "Sending {1} Message: {0}\n".format( type, msg )  )
        message = "|".join( [ type, msg ] )
        self.request_socket.send( message )


class ResponseThread(Thread):
    def __init__(self, init_port ):
        Thread.__init__(self)
        self.cached_results = {}
        self.cached_arrays = {}
        self.socket = self.context.socket(zmq.PULL)
        self.port = bindSocket( self.socket, init_port )
        self.active = True
        self.setName('CDAS Response Thread')
        logger.info( "Connected response socket on port: {0}".format( self.port ) )

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

            except Exception as err:
                logger.error( "CDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ) )

    def term(self):
        self.active = False;
        try: self.socket.close()
        except Exception: pass
