from pycdas.portal.cdas import CDASPortal
import time

try:
    portal = CDASPortal()
    response_manager = portal.createResponseManager()
    portal.start_CDAS()
    time.sleep(4)

    rId = portal.sendMessage("getCapabilities", [ "" ] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:

    portal.shutdown()



