import sys, os, logging, traceback
import time, numpy as np
import zmq, cdms2
from pycdas.messageParser import mParse

class Worker(object):

    def __init__(self, worker_index, request_port, result_port ):
        self.logger = self.getLogger( worker_index )
        self.cached_results = {}
        self.cached_inputs = {}
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


    # def __del__(self):
    #     self.logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
    #     self.request_socket.close()
    #     self.result_socket.close()

    def getMessageType(self, header ):
        self.logger.info(  " Got header: " + str(header) )
        toks = header.split('|')[0]

    def run(self):
        self.logger.info(  " Running: listening for messages" )
        while True:
            try:
                header = self.request_socket.recv()
                type = self.getMessageType(header)
                self.logger.info(  " Worker got message, type = : " + type )
                if type == "array":
                    data = self.request_socket.recv()
                    self.loadVariable(header,data)
                elif type == "task":
                    ""
                    self.result_socket.send(header)

            except Exception as err:
                self.logger.error( "\n-------------------------------\nError getting messsage: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )

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

    def loadVariable( self, header, data ):
        try:
            t0 = time.time()
            self.logger.info(  " *** Got data, nbytes = : " + str(len(data)) )
            header_toks = header.split('|')
            vid = header_toks[1]
            origin = mParse.s2ia( header_toks[2] )
            self.logger.info(  " *** 1 " )
            shape = mParse.s2ia( header_toks[3] )
            metadata = mParse.s2m( header_toks[4] )
            self.logger.info(  " *** 2 " )
            gridFilePath = metadata["gridFile"]
            self.logger.info(  " *** 3 " )
            gridfile = cdms2.open( gridFilePath )
            name = metadata["name"]
            self.logger.info(  " * Got Var name = " + name )
            var = gridfile(name)
            self.logger.info(  " * Got Var object = " + var.id )
            collection = metadata["collection"]
            dimensions = metadata["dimensions"].split(",")
            axes = [ gridfile.axes.get(dim) for dim in dimensions ]
            partition_axes = self.subsetAxes( axes, origin, shape )
            grid = gridfile.grids.values()[0]
            nparray = np.frombuffer( data, dtype='f' ).reshape( shape )
            variable =  cdms2.createVariable( nparray, typecode=None, copy=0, savespace=0, mask=None, fill_value=var.missing, grid=grid, axes=partition_axes, attributes=metadata, id=collection+"-"+name)
            variable.createattribute( "gridfile", gridFilePath )
            t1 = time.time()
            self.logger.info( " >> Created Variable: {0} in time {1}".format( variable.id, (t1-t0) ) )
            self.logger.info( " >> Array Metadata: {0}".format( metadata ) )
            self.logger.info( " >> Array Shape: [{0}]".format( ', '.join( map(str, shape) ) ) )
            self.logger.info( " >> Array Origin: [{0}]".format( ', '.join( map(str, origin) ) ) )
            self.logger.info( " >> Partition Axes: {0}".format(', '.join( [ axis.id for axis in partition_axes ] ) ) )
            self.cached_inputs[vid] = variable
        except Exception as err:
            self.logger.error( "\n-------------------------------\nError creating variable: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )

    def subsetAxes( self, axes, origin, shape ):
        subAxes = []
        for index in range( len(shape) ):
            start = origin[index]
            length = shape[index]
            subAxes.append( axes[index].subAxis( start, start + length) )
        return subAxes

worker_index = mParse.getIntArg(1,0)
request_port = mParse.getIntArg(2, 8200 )
result_port = mParse.getIntArg(3, 8201 )
worker = Worker( worker_index, request_port, result_port )
worker.run()
worker.logger.info(  " ############################## EXITING WORKER ##############################"  )