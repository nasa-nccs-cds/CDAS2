from pycdas.portal.cdas import *
import time, sys

startServer = True
portal = None
request_port = 4356
response_port = 4357

try:

    if startServer:
        portal = CDASPortal()
        portal.start_CDAS()
        time.sleep(6)
    else:
        portal = CDASPortal(ConnectionMode.CONNECT, "localhost", request_port, response_port)

    response_manager = portal.createResponseManager()

    t0 = time.time()
#    datainputs = """[domain=[{"name":"d0"},{"name":"d1","lev":{"start":5,"end":5,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d1"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]"""
#  datainputs = """[domain=[{"name":"d0","lev":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d0"}],operation=[{"name":"numpyModule.max","input":"v1","axes":"tx"}]]"""
    datainputs = """[domain=[{"name":"d0"}],variable=[{"uri":"file://att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/npana.xml","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]"""
    print "Sending request on port {0} (awaiting response): {1}".format( portal.request_port, datainputs ); sys.stdout.flush()
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}, Got responses:\n{1}".format( time.time()-t0, "\n".join(responses) ); sys.stdout.flush()

except Exception, err:
    print "Exception occurred: " + err

finally:

    portal.shutdown()


