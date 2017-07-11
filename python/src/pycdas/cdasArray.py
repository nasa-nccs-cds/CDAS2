import numpy as np
import numpy.ma as ma
import time, traceback, logging, struct, sys
from messageParser import mParse
IO_DType = np.dtype( np.float32 ).newbyteorder('>')
from abc import ABCMeta, abstractmethod

def getFillValue( array ):
    try:    return array.get_fill_value()
    except: return sys.float_info.max

class CDArray:
    __metaclass__ = ABCMeta

    def __init__(self, _id, _origin, _shape, _metadata ):
        self.logger = logging.getLogger("worker")
        self.id = _id
        self.origin = _origin
        self.shape = _shape
        self.metadata = _metadata
        self.roi = self.parseRoi()
        self.logger.debug("Created Array: {0}".format(self.id))
        self.logger.debug(" >> Array Metadata: {0}".format(self.metadata))
        self.logger.debug(" >> Array Shape: [{0}]".format(', '.join(map(str, self.shape))))
        self.logger.debug(" >> Array Origin: [{0}]".format(', '.join(map(str, self.origin))))
        self.logger.debug(" >> Array ROI: [{0}]".format(', '.join(map(str, self.roi.items()))))

    @classmethod
    @abstractmethod
    def createResult(cls, task, input, result_array ): raise Exception( "Executing abstract method createResult in CDArray")

    def uid(self): return self.id.split('-')[0]

    def getGridBounds(self):
        gridBnds = self.metadata.get("gridbnds")
        if ( gridBnds is None ): return None
        else:
            bndVals = [ float(grdBnd) for grdBnd in gridBnds.split(",") ]
            return ( (bndVals[0],bndVals[1]), (bndVals[2],bndVals[3]) )


    @classmethod
    @abstractmethod
    def createInput(cls, header, data): raise Exception( "Executing abstract method createInput in CDArray")

    @abstractmethod
    def getVariable( self, gridFile = None ): pass

    @abstractmethod
    def array(self): pass

    @abstractmethod
    def getGrid(self): pass

    @abstractmethod
    def subsetAxes(self, dimensions, gridfile, origin, shape ): pass

    @abstractmethod
    def toBytes( self, dtype ): pass

    def getAxisSection( self, axis ): return None if self.roi == None else self.roi.get( axis.lower(), None )

    def parseRoi(self):
        roiSpec = self.metadata.get("roi")
        roiMap = {}
        if( roiSpec != None ):
            self.logger.info(" ***->> Parsing ROI spec: {0}".format( roiSpec ) )
            for roiTok in roiSpec.split('+'):
                axisToks = roiTok.split(',')
                roiMap[ axisToks[0].lower() ] = ( int(axisToks[1]),  int(axisToks[2]) + 1 )
        return roiMap

class npArray(CDArray):

    @classmethod
    def createResult(cls, task, input, result_array ):
        return npArray( task.rId, input.origin, result_array.shape, dict( input.metadata, **task.metadata ), result_array, input.undef )

    @classmethod
    def createAuxResult( cls, id, metadata, input, result_array ):
        return npArray( id, input.origin, result_array.shape, metadata, result_array, input.undef )

    def toBytes( self, dtype ):
        return self.array.astype(dtype).tobytes() + np.array([self.undef]).astype(dtype).tobytes() # bytearray(struct.pack("f", self.undef))

    @classmethod
    def createInput(self, header, data):
        logger = logging.getLogger("worker")
        logger.info(" ***->> Creating Input, header = {0}".format( header ) )
        header_toks = header.split('|')
        id = header_toks[1]
        origin = mParse.s2it(header_toks[2])
        shape = mParse.s2it(header_toks[3])
        metadata = mParse.s2m(header_toks[4])
        if data:
            logger.info(" *** Creating Input, id = {0}, data size = {1}, shape = {2}".format( id, len(data), str(shape) ) )
            raw_data = np.frombuffer( data, dtype=IO_DType ).astype(np.float32)
            undef_value = raw_data[-1]
            logger.info(" *** buffer len = {0}, undef = {1}".format( str(len(raw_data)), str(undef_value) ) )
            data_array = ma.masked_invalid( raw_data[0:-1].reshape(shape) )
            nparray =  ma.masked_equal(data_array,undef_value) if ( undef_value != 1.0 ) else data_array
        else:
            nparray = None
            undef_value = float('inf')
        return npArray( id, origin, shape, metadata, nparray, undef_value )

    def __init__(self, _id, _origin, _shape, _metadata, _ndarray, _undef ):
        super(npArray, self).__init__(_id,_origin,_shape,_metadata)
        self.gridFile = self.metadata["gridfile"]
        self.name = self.metadata.get("name","")
        self.collection = self.metadata.get("collection","")
        self.dimensions = self.metadata.get("dimensions","").split(",")
        self.array = _ndarray
        self.undef = _undef
        self.variable = None
        self.logger.info(" *** Creating NP data array, nbytes = " + str(self.nbytes()) + ", undef = " + str(self.undef) )

    def getSelector(self, variable, **args):
        kargs = {}
        for idim in range( variable.rank() ):
            axis = variable.getAxis(idim)
            start = self.origin[idim]
            end = start + self.shape[idim]
            interval = [ start, end ]
            kargs[axis.id] = slice(*interval)
        return kargs

    def nbytes(self): return self.array.nbytes if (self.array != None) else 0
    def array(self): return self.array

    def getGrid1(self):
        import cdms2
        gridfile = cdms2.open(self.gridFile)
        baseGrid = gridfile.grids.values()[0]
        gridBnds = self.getGridBounds()
        if ( gridBnds is None ):  return baseGrid
        else:
            variable = self.getVariable()
            (lataxis, lonaxis) = (variable.getLatitude(), variable.getLongitude())
            (latInterval, lonInterval) = (lataxis.mapInterval( gridBnds[0] ), lonaxis.mapInterval( gridBnds[1] ))
            return baseGrid.subGrid( latInterval, lonInterval )

    def getGrid(self):
        import cdms2
        gridfile = cdms2.open(self.gridFile)
        baseGrid = gridfile.grids.values()[0]
        (latInterval, lonInterval) = ( self.getAxisSection('y'), self.getAxisSection('x') )
        if ( (latInterval == None) or (lonInterval == None)  ):  return baseGrid
        else: return baseGrid.subGrid( latInterval, lonInterval )

    def getVariable( self, gridFilePath = None ):
        import cdms2
        if( self.variable is None ):
            t0 = time.time()
            gridfile = cdms2.open( self.gridFile if (gridFilePath==None) else gridFilePath )
            var = gridfile[self.name]
            grid = gridfile.grids.values()[0]
            partition_axes = self.subsetAxes(self.dimensions, gridfile, self.origin, self.shape)
            self.variable = cdms2.createVariable(self.array, typecode=None, copy=0, savespace=0, mask=None, fill_value=var.getMissing(),
                                            grid=grid, axes=partition_axes, attributes=self.metadata, id=self.collection + "-" + self.name)
            self.variable.createattribute("gridfile", self.gridFile)
            self.variable.createattribute("origin", mParse.ia2s(self.origin))
            t1 = time.time()
            self.logger.info(" >> Created CDMS Variable: {0} ({1}) in time {2}, gridFile = {3}".format(self.variable.id, self.name, (t1 - t0), self.gridFile))
        return self.variable

    def subsetAxes( self, dimensions, gridfile, origin, shape ):
        subAxes = []
        try:
            for index in range( len(dimensions) ):
                start = origin[index]
                length = shape[index]
                dim = dimensions[index]
                axis = gridfile.axes.get(dim)
                subAxes.append( axis.subAxis( start, start + length ) )
                self.logger.info( " >> Axis: {0}, length: {1} ".format( dim, length ) )
        except Exception as err:
            self.logger.info( "\n-------------------------------\nError subsetting Axes: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )
            raise err
        return subAxes


class cdmsArray(CDArray):

    @classmethod
    def createResult(cls, task, input, cdVariable ):
        return cdmsArray( task.rId, input.origin, cdVariable.shape, dict( input.metadata, **task.metadata ), cdVariable )

    @classmethod
    def getName(cls, variable ):
        try:        return variable.name_in_file;
        except:     return variable.id;


    @classmethod
    def createInput( cls, cdVariable ):
        id = cdVariable.id
        origin = cdVariable.attributes.get("origin")
        shape = cdVariable.shape
        metadata = cdVariable.attributes
        return cdmsArray( id, origin, shape, metadata, cdVariable )

    def array(self):
        return self.variable.data

    def toBytes( self, dtype ):
        return self.variable.data.astype(dtype).tobytes() + np.array([ self.variable.getMissing() ]).astype(dtype).tobytes() # bytearray( struct.pack("f", self.variable.getMissing()))

    def __init__(self, _id, _origin, _shape, _metadata, cdVariable ):
        super(cdmsArray, self).__init__(_id,_origin,_shape,_metadata)
        self.logger.info(" *** Creating input cdms array, size = " + str( cdVariable.size ) )
        self.name = cdmsArray.getName(cdVariable)
        self.grid = cdVariable.getGrid()
        self.dimensions = self.metadata["dimensions"].split(",")
        self.variable = cdVariable

    def getVariable( self, gridFile= None ): return self.variable

    def getGrid(self):
        baseGrid = self.variable.getGrid()
        (latInterval, lonInterval) = ( self.getAxisSection('y'), self.getAxisSection('x') )
        if ( (latInterval == None) or (lonInterval == None)  ):  return baseGrid
        else: return baseGrid.subGrid( latInterval, lonInterval )

    def getGrid1(self):
        gridBnds = self.getGridBounds()
        if ( gridBnds == None ):  return self.variable.getGrid()
        else:
            (lataxis, lonaxis) = (self.variable.getLatitude(), self.variable.getLongitude())
            (latInterval, lonInterval) = (lataxis.mapInterval( gridBnds[0] ), lonaxis.mapInterval( gridBnds[1] ))
            self.logger.info( " latInterval {0} --- lonInterval {1} ".format( str(latInterval), str(lonInterval) ) )
            return self.variable.getGrid().subGrid( latInterval, lonInterval )

    def subsetAxes( self, dimensions, gridfile, origin, shape ):
        subAxes = []
        try:
            for index in range( len(dimensions) ):
                start = origin[index]
                length = shape[index]
                dim = dimensions[index]
                axis = gridfile.axes.get(dim)
                subAxes.append( axis.subAxis( start, start + length ) )
                self.logger.info( " >> Axis: {0}, length: {1} ".format( dim, length ) )
        except Exception as err:
            self.logger.info( "\n-------------------------------\nError subsetting Axes: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )
            raise err
        return subAxes


