import numpy as np
import time
from messageParser import mParse

class CDArray:
    def __init__(self, header, data):
        self.logger.info(" *** Creating data array, nbytes = : " + str(len(data)))
        try:
            header_toks = header.split('|')
            self.id = header_toks[1]
            self.origin = mParse.s2ia(header_toks[2])
            self.shape = mParse.s2ia(header_toks[3])
            self.metadata = mParse.s2m(header_toks[4])
            self.gridFilePath = self.metadata["gridfile"]
            self.name = self.metadata["name"]
            self.collection = self.metadata["collection"]
            self.dimensions = self.metadata["dimensions"].split(",")
        except  Exception as err:
            self.logger.info( "Metadata Error: {0}\ntoks: {1}\nmdata: {2}".format(err, ', '.join(header_toks), str(self.metadata)))
            raise err
        self.array = np.frombuffer(data, dtype=self.io_dtype).reshape(self.shape).astype(np.float32)
        self.logger.info(" >> Array Metadata: {0}".format(self.metadata))
        self.logger.info(" >> Array Shape: [{0}]".format(', '.join(map(str, self.array.shape))))
        self.logger.info(" >> Array Dimensions: [{0}]".format(', '.join(map(str, self.dimensions))))
        self.logger.info(" >> Array Origin: [{0}]".format(', '.join(map(str, self.origin))))

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
        self.logger.info(" >> Created CDMS Variable: {0} ({1} in time {2}".format(variable.id, self.name, (t1 - t0)))
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