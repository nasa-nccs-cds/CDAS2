from pycdas.portal.cdas import *
import time

# assumes the java/scala side of the CDASPortal has been started using the startupCDASPortal.py script.

request_port = 5670
response_port = 5671

try:
    portal = CDASPortal( ConnectionMode.CONNECT, "10.71.9.11", request_port, response_port )
    response_manager = portal.createResponseManager()

    datainputs = """[domain=[{"name":"d0","time":{"start":0,"end":30,"system":"indices"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/cdas/cache/collections/NCML/CIP_MERRA2_6hr_tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId = portal.sendMessage("execute", [ "WPS", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:
    portal.shutdown()


