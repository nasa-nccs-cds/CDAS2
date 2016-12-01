import sys, numpy as np

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