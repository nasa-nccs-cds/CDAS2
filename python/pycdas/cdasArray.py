import numpy as np
import time, traceback
from messageParser import mParse
from kernels.Kernel import logger
IO_DType = np.dtype( np.float32 ).newbyteorder('>')
from abc import ABCMeta, abstractmethod

class CDArray:
    __metaclass__ = ABCMeta

    def __init__(self, _id, _origin, _shape, _metadata ):
        self.id = _id
        self.origin = _origin
        self.shape = _shape
        self.metadata = _metadata
        logger.debug("Created Array: {0}".format(self.id))
        logger.debug(" >> Array Metadata: {0}".format(self.metadata))
        logger.debug(" >> Array Shape: [{0}]".format(', '.join(map(str, self.shape))))
        logger.debug(" >> Array Origin: [{0}]".format(', '.join(map(str, self.origin))))

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
    def createInput(cls, header, data):
        logger.info(" *** Creating data array, nbytes = : " + str(len(data)))
        header_toks = header.split('|')
        id = header_toks[1]
        origin = mParse.s2it(header_toks[2])
        shape = mParse.s2it(header_toks[3])
        metadata = mParse.s2m(header_toks[4])
        nparray = np.frombuffer( data, dtype=IO_DType ).reshape(shape).astype(np.float32)
        return npArray( id, origin, shape, metadata, nparray )

    def __init__(self, _id, _origin, _shape, _metadata, _ndarray ):
        super(npArray, self).__init__(_id,_origin,_shape,_metadata)
        logger.info(" *** Creating data array, nbytes = " + str( _ndarray.nbytes ) )
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
        partition_axes = self.subsetAxes(self.dimensions, gridfile, self.origin, self.shape)
        variable = cdms2.createVariable(self.array, typecode=None, copy=0, savespace=0, mask=None, fill_value=var.getMissing(),
                                        grid=grid, axes=partition_axes, attributes=self.metadata, id=self.collection + "-" + self.name)
        variable.createattribute("gridfile", self.gridFilePath)
        variable.createattribute("origin", mParse.ia2s(self.origin))
        t1 = time.time()
        logger.info(" >> Created CDMS Variable: {0} ({1} in time {2}".format(variable.id, self.name, (t1 - t0)))
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
                logger.info( " >> Axis: {0}, length: {1} ".format( dim, length ) )
        except Exception as err:
            logger.info( "\n-------------------------------\nError subsetting Axes: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )
            raise err
        return subAxes


class cdmsArray(CDArray):

    @classmethod
    def createResult(cls, task, input, result_array ):
        return cdmsArray( task.rId, input.origin, result_array.shape, dict( input.metadata, **task.metadata ), result_array )


    @classmethod
    def createInput( cls, cdVariable ):
        logger.info(" *** Creating input cdms array, size = : " + str( cdVariable.size ) )
        id = cdVariable.id
        origin = cdVariable.attributes.get("origin")
        shape = cdVariable.shape
        metadata = cdVariable.attributes
        return cdmsArray( id, origin, shape, metadata, cdVariable )


    def __init__(self, _id, _origin, _shape, _metadata, cdVariable ):
        super(cdmsArray, self).__init__(_id,_origin,_shape,_metadata)
        logger.info(" *** Creating input cdms array, size = " + str( cdVariable.size ) )
        self.name = cdVariable.name_in_file
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
                logger.info( " >> Axis: {0}, length: {1} ".format( dim, length ) )
        except Exception as err:
            logger.info( "\n-------------------------------\nError subsetting Axes: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )
            raise err
        return subAxes