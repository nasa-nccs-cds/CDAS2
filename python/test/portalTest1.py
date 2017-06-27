from pycdas.portal.cdas import *
import time

# assumes the java/scala side of the CDASPortal has been started using the startupCDASPortal.py script.

request_port = 5670
response_port = 5671

try:
    portal = CDASPortal( ConnectionMode.CONNECT, "localhost", request_port, response_port )
    response_manager = portal.createResponseManager()

    datainputs = """[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId = portal.sendMessage("execute", [ "WPS", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:
    portal.shutdown()


