import sys, os, logging, traceback
import time, numpy as np
import zmq, cdms2

class MessageParser:

    def getIntArg( self, index, default ): return int(sys.argv[index]) if index < len( sys.argv ) else default

    def s2m( self, mdataStr ):
        metadata = {}
        for item in mdataStr.split(";"):
            toks = item.split(":")
            if len(toks) == 2:
                metadata[ toks[0] ] = toks[1]
        return metadata

    def s2ia(self, mdataStr ):
        return np.asarray( [ int(item) for item in mdataStr.split(',') ] )



mParse = MessageParser()

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


    def __del__(self): self.shutdown()

    def shutdown(self):
        self.logger.info(  " ############################## SHUT DOWN DATA SOCKET ##############################"  )
        self.request_socket.close()
        self.result_socket.close()

    def getMessageField(self, header, index ):
        self.logger.info(  " Got header: " + str(header) )
        toks = header.split('|')
        return toks[index]

    def run(self):
        self.logger.info(  " Running: listening for messages" )
        active = True
        while active:
            try:
                header = self.request_socket.recv()
                type = self.getMessageField(header,0)
                self.logger.info(  " Worker got message, type = " + type )
                if type == "array":
                    data = self.request_socket.recv()
                    self.loadVariable(header,data)

                elif type == "task":
                    self.logger.info(  " Got task request, header = " + header + ", cached inputs = " + ', '.join( self.cached_inputs.keys() ) )
                    result = self.processTask( header )
                    self.result_socket.send(result)

                elif type == "quit":
                    self.logger.info(  " Quitting main thread. " )
                    active = False

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

    def processTask(self, task_header ):
        t0 = time.time()
        header_toks = task_header.split('|')
        taskToks = header_toks[1].split('-')
        opToks = taskToks[0].split('.')
        module = opToks[0]
        op = opToks[1]
        rId = taskToks[1]
        inpitIds = header_toks[2].split(',')
        metadata = mParse.s2m( header_toks[3] )
        inputs = [ self.cached_inputs.get( inputId ) for inputId in inpitIds ]

        if( op == "regrid" ):
            crsToks = metadata.get("crs","gaussian~128").split("~")
            regridder = metadata.get("regridder","regrid2")
            crs = crsToks[0]
            resolution = int(crsToks[1]) if len(crsToks) > 1 else 128
            if crs == "gaussian":
                t42 = cdms2.createGaussianGrid( resolution )
                results = [ input.regrid( t42 , regridTool=regridder ) for input in inputs ]
                self.logger.info( " >> Regridded variables in time {0}, nresults = {1}".format( (time.time()-t0), len(results) ) )
                return results



    def loadVariable( self, header, data ):
        try:
            t0 = time.time()
            self.logger.info(  " *** Got data, nbytes = : " + str(len(data)) )
            header_toks = header.split('|')
            vid = header_toks[1]
            origin = mParse.s2ia( header_toks[2] )
            shape = mParse.s2ia( header_toks[3] )
            metadata = mParse.s2m( header_toks[4] )
            gridFilePath = metadata["gridFile"]
            gridfile = cdms2.open( gridFilePath )
            name = metadata["name"]
            var = gridfile[name]
            collection = metadata["collection"]
            dimensions = metadata["dimensions"].split(",")
            axes = [ gridfile.axes.get(dim) for dim in dimensions ]
            grid = gridfile.grids.values()[0]
            nparray = np.frombuffer( data, dtype='f' ).reshape( shape )

            self.logger.info( " >> Array Metadata: {0}".format( metadata ) )
            self.logger.info( " >> Array Shape: [{0}]".format( ', '.join( map(str, shape) ) ) )
            self.logger.info( " >> Array Dimensions: [{0}]".format( ', '.join( map(str, dimensions) ) ) )
            self.logger.info( " >> Array Origin: [{0}]".format( ', '.join( map(str, origin) ) ) )

            partition_axes = self.subsetAxes( axes, origin, shape )
            variable =  cdms2.createVariable( nparray, typecode=None, copy=0, savespace=0, mask=None, fill_value=var.getMissing(), grid=grid, axes=partition_axes, attributes=metadata, id=collection+"-"+name)
            variable.createattribute( "gridfile", gridFilePath )
            t1 = time.time()
            self.logger.info( " >> Created Variable: {0} ({1} in time {2}".format( variable.id, name,  (t1-t0) ) )
            self.cached_inputs[vid] = variable
        except Exception as err:
            self.logger.error( "\n-------------------------------\nError creating variable: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )

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

worker_index = mParse.getIntArg(1,0)
request_port = mParse.getIntArg(2, 8200 )
result_port = mParse.getIntArg(3, 8201 )
worker = Worker( worker_index, request_port, result_port )
worker.run()
worker.logger.info(  " ############################## EXITING WORKER ##############################"  )