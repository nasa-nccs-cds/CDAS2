import sys
import time
import zmq

def getIntArg( index, default ): return int(sys.argv[index]) if index < len( sys.argv ) else default

class Worker(object):

    def __init__(self, worker_index, request_port, result_port ):
        self.logger = self.getLogger( worker_index )
        self.context = zmq.Context()

        self.request_socket = self.context.socket(zmq.PULL)
        self.request_socket.bind("tcp://localhost:" + str(request_port) )
        self.logger.info( "Connected request socket on port: {0}".format( request_port ) )

        self.result_socket = self.context.socket(zmq.PUSH)
        self.result_socket.bind("tcp://localhost:" + str(result_port) )
        self.logger.info( "Connected result socket on port: {0}".format( result_port ) )

        self.cached_results = {}

    def __del__(self):
        self.logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.request_socket.close()
        self.result_socket.close()

    def run(self):
        while True:
            header = self.request_socket.recv()
            self.logger.info(  " Got header: " + str(header) )
            data = self.request_socket.recv()
            self.logger.info(  " Got data, nbytes = : " + str(len(data)) )
            self.result_socket.send(header)

worker_index = getIntArg(1,0)
request_port = getIntArg(2, 8200 )
result_port = getIntArg(3, 8201 )
worker = Worker( worker_index, request_port, result_port )