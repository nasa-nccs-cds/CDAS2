import numpy as np
import numpy.ma as ma
import time, traceback, logging
from messageParser import mParse
IO_DType = np.dtype( np.float32 ).newbyteorder('>')
from abc import ABCMeta, abstractmethod

class CDArray:
    __metaclass__ = ABCMeta

    def __init__(self, _id, _origin, _shape, _metadata ):
        self.logger = logging.getLogger("worker")
        self.id = _id
        self.origin = _origin
        self.shape = _shape
        self.metadata = _metadata
        self.logger.debug("Created Array: {0}".format(self.id))
        self.logger.debug(" >> Array Metadata: {0}".format(self.metadata))
        self.logger.debug(" >> Array Shape: [{0}]".format(', '.join(map(str, self.shape))))
        self.logger.debug(" >> Array Origin: [{0}]".format(', '.join(map(str, self.origin))))

    @classmethod
    @abstractmethod
    def createResult(cls, task, input, result_array ): raise Exception( "Executing abstract method createResult in CDArray")

    @classmethod
    @abstractmethod
    def createInput(cls, header, data): raise Exception( "Executing abstract method createInput in CDArray")

    @abstractmethod
    def getVariable(self): pass

    @abstractmethod
    def subsetAxes(self): pass



class npArray(CDArray):

    @classmethod
    def createResult(cls, task, input, result_array ):
        return npArray( task.rId, input.origin, result_array.shape, dict( input.metadata, **task.metadata ), result_array )

    @classmethod
    def createInput(self, header, data):
        logger = logging.getLogger("worker")
        header_toks = header.split('|')
        id = header_toks[1]
        origin = mParse.s2it(header_toks[2])
        shape = mParse.s2it(header_toks[3])
        metadata = mParse.s2m(header_toks[4])
        raw_data = np.frombuffer( data, dtype=IO_DType ).astype(np.float32)
        logger.info(" *** Creating Input, id = {0}, buffer len = {1}, shape = {2}, undef = {3}".format( id, str(len(raw_data)), str(shape), str(raw_data[-1]) ) )
        data_array = raw_data[0:-1].reshape(shape)
        undef_value = raw_data[-1]
        nparray = ma.masked_equal(data_array,undef_value) if ( undef_value != 1.0 ) else data_array
        return npArray( id, origin, shape, metadata, nparray )

    def __init__(self, _id, _origin, _shape, _metadata, _ndarray ):
        super(npArray, self).__init__(_id,_origin,_shape,_metadata)
        self.logger.info(" *** Creating data array, nbytes = " + str( _ndarray.nbytes ) )
        self.gridFilePath = self.metadata["gridfile"]
        self.name = self.metadata["name"]
        self.collection = self.metadata["collection"]
        self.dimensions = self.metadata["dimensions"].split(",")
        self.array = _ndarray

    def getVariable(self):
        import cdms2
        t0 = time.time()
        gridfile = cdms2.open(self.gridFilePath)
        var = gridfile[self.name]
        grid = gridfile.grids.values()[0]
        grid1 = var.getGrid()
        partition_axes = self.subsetAxes(self.dimensions, gridfile, self.origin, self.shape)
        variable = cdms2.createVariable(self.array, typecode=None, copy=0, savespace=0, mask=None, fill_value=var.getMissing(),
                                        grid=grid, axes=partition_axes, attributes=self.metadata, id=self.collection + "-" + self.name)
        variable.createattribute("gridfile", self.gridFilePath)
        variable.createattribute("origin", mParse.ia2s(self.origin))
        t1 = time.time()
        inlatBounds, inlonBounds = grid1.getBounds()
        self.logger.info(" >> Created CDMS Variable: {0} ({1}) in time {2}, gridFile = {3}, lat bounds = {4}".format(variable.id, self.name, (t1 - t0), self.gridFilePath, str(inlatBounds) ))
        return variable

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
        return self.variable.data.astype(dtype).tobytes()

    def __init__(self, _id, _origin, _shape, _metadata, cdVariable ):
        super(cdmsArray, self).__init__(_id,_origin,_shape,_metadata)
        self.logger.info(" *** Creating input cdms array, size = " + str( cdVariable.size ) )
        self.name = cdmsArray.getName(cdVariable)
        self.grid = cdVariable.getGrid()
        self.dimensions = self.metadata["dimensions"].split(",")
        self.variable = cdVariable

    def getVariable(self): return self.variable

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


