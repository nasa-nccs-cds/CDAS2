from pycdas.portal.cdas import CDASPortal
import time

startServer = False

try:
    portal = CDASPortal()
    response_manager = portal.createResponseManager()
    if startServer:
        portal.start_CDAS()
        time.sleep(6)

    t0 = time.time()
    datainputs = """[domain=[{"name":"d0","lat":{"start":205,"end":210,"system":"indices"},"lon":{"start":205,"end":215,"system":"indices"}},{"name":"d1","lev":{"start":5,"end":5,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d1"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]"""
    print "Sending request (awaiting response): '" + datainputs + "'"
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}, Got responses:\n{1}".format( time.time()-t0, "\n".join(responses) )

finally:

    portal.shutdown()


