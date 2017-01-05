import zmq, traceback, time, logging
from threading import Thread
from pycdas.cdasArray import npArray
from subprocess import Popen
import shlex

class ConnectionMode():
    BIND = 1
    CONNECT = 2
    DefaultPort = 4336

    @classmethod
    def bindSocket( cls, socket, port ):
        if port > 0:
            socket.bind("tcp://*:{0}".format(port))
        else:
            test_port = cls.DefaultPort
            while( True ):
                try:
                    socket.bind( "tcp://*:{0}".format(test_port) )
                    return test_port
                except Exception as err:
                    test_port = test_port + 1

    @classmethod
    def connectSocket( cls, socket, host, port ):
        socket.connect("tcp://{0}:{1}".format( host, port ) )
        return port

class CDASPortal:

    def __init__( self, connectionMode = ConnectionMode.BIND, host="localhost", request_port=0, response_port=0 ):
        try:
            self.logger =  logging.getLogger("portal")
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.PUSH)
            self.app_host = host
            if( connectionMode == ConnectionMode.BIND ):
                self.request_port = ConnectionMode.bindSocket( self.request_socket, request_port )
                self.logger.info( "Binding request socket to port: {0}".format( self.request_port ) )
            else:
                self.request_port = ConnectionMode.connectSocket(self.request_socket, self.app_host, request_port)
                self.logger.info("Connected request socket to server {0} on port: {1}".format( self.app_host, self.request_port ) )

            self.response_thread = ResponseThread( connectionMode, host, response_port )
            self.response_thread.start()
            self.application_thread = None

        except Exception as err:
            err_msg =  "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
            self.logger.error(err_msg)
            print err_msg
            self.shutdown()

    def __del__(self): self.shutdown()

    def start_CDAS(self):  # Stage the CDAS app using the "{CDAS_HOME}>> sbt stage" command.
        self.application_thread = AppThread( self.app_host, self.request_port, self.response_thread.port )
        self.application_thread.start()

    def shutdown(self):
        self.logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.sendMessage("shutdown")
        try: self.request_socket.close()
        except Exception: pass
        if( self.application_thread ):
            self.application_thread.term()
        self.response_thread.term()

    def sendMessage( self, type, msgStrs = [""] ):
        self.logger.info( "Sending {0} request {1} on port {2}.".format( type, msgStrs, self.request_port )  )
        self.request_socket.send( "!".join( [type] + msgStrs ) )

    def join(self):
        self.response_thread.join()

    def waitForResponse(self):
        while( True ):
            response = self.response_thread.popResponse()
            if response:
                self.logger.info(" Received response: " + response )
                return response
            else: time.sleep(0.25)

class AppThread(Thread):
    def __init__(self, host, request_port, response_port):
        Thread.__init__(self)
        self.logger = logging.getLogger("portal")
        self._response_port = response_port
        self._request_port = request_port
        self._host = host

    def run(self):
        cdas_startup = "cdas2 connect {0} {1} -J-Xmx32000M -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:MaxPermSize=800M".format( self._request_port, self._response_port )
        self.process = Popen( shlex.split( cdas_startup ) )
        self.logger.info( "Staring CDAS with command: {0}\n".format( cdas_startup ) )

    def term(self):
        self.process.terminate()

    def join(self):
        self.process.wait()

class ResponseThread(Thread):
    def __init__(self, connectionMode, host, port ):
        Thread.__init__(self)
        self.logger = logging.getLogger("portal")
        self.context = zmq.Context()
        self.cached_results = []
        self.cached_arrays = {}
        self.socket = self.context.socket(zmq.PULL)
        if (connectionMode == ConnectionMode.BIND):
            self.port = ConnectionMode.bindSocket( self.socket, port )
        else:
            self.port = ConnectionMode.connectSocket(self.socket, host, port)
        self.active = True
        self.setName('CDAS Response Thread')
        self.logger.info( "Started response socket on port: {0}".format( self.port ) )

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def popResponse(self):
        if( len( self.cached_results ) == 0 ):
            return None
        else:
            return self.cached_results.pop()

#    def getResponse(self, key, default = None ):
#       return self.cached_results.get( key, default )

    def run(self):
        while( self.active ):
            try:
                response = self.socket.recv()
                toks = response.split('!')
                type = toks[0]
                if type == "array":
                    header = toks[1]
                    data = self.request_socket.recv()
                    array = npArray.createInput(header,data)
                    self.cached_arrays[array.id] = array

                elif type == "response":
                    self.cached_results.append( toks[1] )
                    self.logger.info("Received result: {0}".format(toks[1]))
                else:
                    self.logger.error("CDASPortal.ResponseThread-> Received unrecognized message type: {0}".format(type))

            except Exception as err:
                self.logger.error( "CDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ) )

    def term(self):
        self.active = False;
        try: self.socket.close()
        except Exception: pass
