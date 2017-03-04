from pycdas.portal.cdas import CDASPortal
import time

try:
    portal = CDASPortal()
    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = """[domain=[{"name":"d0","lat":{"start":205,"end":210,"system":"indices"}},{"name":"d1","lon":{"start":205,"end":215,"system":"indices"},"lev":{"start":5,"end":5,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d1","axes":"t"}]]"""
    rId = portal.sendMessage("execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}, Got responses:\n{1}".format( time.time()-t0, "\n".join(responses) )

finally:

    portal.shutdown()


