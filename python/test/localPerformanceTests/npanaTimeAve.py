from pycdas.portal.cdas import *
import time, sys

startServer = False
portal = None
request_port = 5670
response_port = 5671

try:

    if startServer:
        portal = CDASPortal()
        portal.start_CDAS()
        time.sleep(20)
    else:
        portal = CDASPortal(ConnectionMode.CONNECT, "localhost", request_port, response_port)

    response_manager = portal.createResponseManager()

    t0 = time.time()
#    datainputs = """[domain=[{"name":"d0"}],variable=[{"uri":"file:/Users/tpmaxwel/.cdas/cache/collections/NCML/giss_r1i1p1.xml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"tz"}]]"""
#    datainputs = """[domain=[{"name":"d0","lev":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"tas:v1","domain":"d0"}],operation=[{"name":"numpyModule.max","input":"v1","axes":"tx"}]]"""
#    datainputs = """[domain=[{"name":"d0","time":{"start":0,"end":1000,"system":"indices"}}],variable=[{"uri":"file://att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/npana.xml","name":"T:v1","domain":"d0"}],operation=[{"name":"python.cdmsModule.ave","input":"v1","domain":"d0","axes":"tz"}]]"""
    datainputs = """[domain=[{"name":"d0","time":{"start":0,"end":100,"system":"indices"}}],variable=[{"uri":"file://att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/npana.xml","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"tz"}]]"""
    print "Sending request on port {0} (awaiting response!!): {1}".format( portal.request_port, datainputs ); sys.stdout.flush()
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}".format( time.time()-t0 ); sys.stdout.flush()

except Exception, err:
    print "Exception occurred: " + err

finally:

    portal.shutdown()


