from pycdas.portal.cdas import *
import time, sys

startServer = False
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
    datainputs = """[domain=[{"name":"d0","lat":{"start":205,"end":210,"system":"indices"},"lon":{"start":205,"end":215,"system":"indices"}},{"name":"d1","lev":{"start":5,"end":5,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d1"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]"""
    print "Sending request on port {0} (awaiting response): {1}".format( portal.request_port, datainputs ); sys.stdout.flush()
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}, Got responses:\n{1}".format( time.time()-t0, "\n".join(responses) ); sys.stdout.flush()

except Exception, err:
    print "Exception occurred: " + err

finally:

    portal.shutdown()


