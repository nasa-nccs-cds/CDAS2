import json
import numpy
import time


def wrank( value ):
    return ( value if not isinstance(value,basestring) else int( value.split('-')[1] ) )

def wid( value ):
    return ( value if not isinstance(value,int) else 'W-%d' % value )

def unwrap( results ):
    while True:
        if isinstance( results, list ) and len( results ) == 1: results = results[0]
        else: break
    return results

def debug_trace():
    import pydevd
    pydevd.settrace('localhost', port=8333, stdoutToServer=True, stderrToServer=True)

def debug_stop():
    try:
        import pydevd
        pydevd.stoptrace()
    except: pass

def filter_attributes( attr, keys, include_keys = True ):
    rv = {}
    for key in attr.iterkeys():
        if ( include_keys and (key in keys) ) or (not include_keys and (key not in keys)):
            rv[key] = attr[key]
    return rv

def get_json_arg( id, args, default=None ):
        json_arg = args.get( id, None )
        if json_arg is not None: return convert_json_str( json_arg )
        return default if json_arg is None else str(json_arg)

def convert_json_str( json_arg ):
        if isinstance(json_arg, basestring):
            json_arg = str(json_arg).replace("u'","'")
            json_arg = str(json_arg).replace("'",'"')
            return json.loads(json_arg)
        else:
            return json_arg

def genericize( results ):
    result_list = results if isinstance( results, list ) else [ results ]
    for result in result_list:
        for key,value in result.iteritems():
            if type(value) not in [ dict, list, str, tuple ]: result[key] = str(value)

def dump_json_str( obj ):
    return json.dumps(obj) if not isinstance(obj, basestring) else obj

class ExecutionRecord:

    def __init__( self, stats = None ):
        if stats is None: self.clear()
        elif isinstance( stats, dict ):
            self.rec = stats
        elif isinstance( stats, basestring ):
            self.rec = json.loads(stats)
        else:
            raise Exception( "Unrecognized stats in ExecutionRecord: %s" % str(stats) )

    def addRecs( self, **kwargs ):
        self.rec.update( kwargs )

    def clear(self):
        self.rec = {}

    def toJson(self):
        return json.dumps(self.rec)

    def __str__(self):
        return str( self.rec )

    def items(self):
        return self.rec.items()

    def iteritems(self):
        return self.rec.iteritems()

    def find(self, *keys ):
        for key in keys:
            value = self.rec.get( key, None )
            if value is not None: return value
        return None

    def __getitem__(self, item):
        return self.rec.get( item, None )

    def __setitem__(self, key, value):
        self.rec[ key ] = value

def record_attributes( var, attr_name_list, additional_attributes = {} ):
    mdata = {}
    for attr_name in attr_name_list:
        if attr_name == '_data_' and hasattr(var,"getValue"):
            attr_val =  var.getValue()
        else:
            attr_val = var.__dict__.get(attr_name,None)
        if attr_val is None:
            attr_val = var.attributes.get(attr_name,None)
        if attr_val is not None:
            if isinstance( attr_val, numpy.ndarray ):
                attr_val = attr_val.tolist()
            mdata[attr_name] = attr_val
    for attr_name in additional_attributes:
        mdata[attr_name] = additional_attributes[attr_name]
    return mdata

class Profiler(object):

    def __init__(self):
        self.t0 = None
        self.marks = []

    def mark( self, label="" ):
        if self.t0 is None:
           self.t0 = time.time()
        else:
            t1 = time.time()
            self.marks.append(  (label, (t1-self.t0) ) )
            self.t0 = t1

    def dump( self, title ):
        print title
        for mark in self.marks:
            print " %s: %.4f " % ( mark[0], mark[1] )


def location2cdms(region):
    kargs = {}
    for k,v in region.iteritems():
        if k not in ["id","version"]:
            kargs[str(k)] = float( str(v) )
    return kargs

def region2cdms( region, **args ):
    kargs = {}
    if region:
        for k,v in region.iteritems():
            if k in ["id","version"]:
                continue
            if isinstance( v, float ) or isinstance( v, int ):
                buffer = args.get('buffer',False)
                kargs[str(k)] = (v-0.0001,v+0.0001,"cob") if buffer else (v,v,"cob")
            elif isinstance( v, list ) or isinstance( v, tuple ):
                kargs[str(k)] = ( float(v[0]), float(v[1]), "cob" )
            else:
                system = v.get("system","value").lower()
                if isinstance(v["start"],unicode):
                    v["start"] = str(v["start"])
                if isinstance(v["end"],unicode):
                    v["end"] = str(v["end"])
                if system == "value":
                    kargs[str(k)]=(v["start"],v["end"])
                elif system == "index":
                    kargs[str(k)] = slice(v["start"],v["end"])
    return kargs

if __name__ == "__main__":
    from kernels.OperationsManager import cdasOpManager
    capabilities = cdasOpManager.getCapabilitiesStr()
    print capabilities.replace("|","\n").replace("!","\t")