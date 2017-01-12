from pycdas.portal.cdas import CDASPortal
import time

try:
    portal = CDASPortal()
    response_manager = portal.createResponseManager()
    portal.start_CDAS()
    time.sleep(4)

    datainputs = """[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId = portal.sendMessage("execute", [ "CDSpark.max", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:

    portal.shutdown()


