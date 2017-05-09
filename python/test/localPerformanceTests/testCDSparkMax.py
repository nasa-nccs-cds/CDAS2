from pycdas.portal.cdas import *
import time, sys

startServer = False
portal = None
request_port = 4356
response_port = 4357
server = "localhost"

try:

    if startServer:
        portal = CDASPortal()
        portal.start_CDAS()
        time.sleep(20)
    else:
        portal = CDASPortal(ConnectionMode.CONNECT, server, request_port, response_port)

    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = '[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"file:///home/tpmaxwel/.cdas/cache/collections/NCML/GISS_E2H_r1i1p1.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"t"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, server, datainputs ); sys.stdout.flush()
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}".format( time.time()-t0 ); sys.stdout.flush()
    print "Responses = " + str(responses)

except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


