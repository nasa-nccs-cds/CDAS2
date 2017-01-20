from  pycdas.portal import cdas
import time, sys

request_port = 4356
response_port = 4357

try:
    portal = cdas.CDASPortal(cdas.ConnectionMode.CONNECT, request_port=request_port, response_port=response_port)

    response_manager = portal.createResponseManager()

    request_id = portal.sendMessage('getCapabilities', ['WPS'])

    responses = response_manager.getResponses(request_id)
    print "Got responses:\n" + "\n".join(responses)

finally:
    portal.shutdown()