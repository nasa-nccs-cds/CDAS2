from pycdas.portal.cdas import CDASPortal
import time

try:
    portal = CDASPortal()
    response_manager = portal.createResponseManager()
    portal.start_CDAS()
    time.sleep(4)

    datainputs = """[domain=[{"name":"d0"}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]"""

    rId = portal.sendMessage("execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:

    portal.shutdown()


