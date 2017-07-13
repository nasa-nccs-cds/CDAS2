from pycdas.portal.cdas import *

request_port = 5670
response_port = 5671
cdas_server = "10.71.9.11"

try:
    portal = CDASPortal( ConnectionMode.CONNECT, cdas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","time":{"start":0,"end":3000,"system":"values"},"lat":{"start":70,"end":90,"system":"values"},"lon":{"start":25,"end":45,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/cdas/cache/collections/NCML/MERRA-TAS1hr.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","cycle":"diurnal","bin":"month","axes":"t"}]]"""
    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)

    print " Received " + str( len(responses) ) + " responses "

finally:
    portal.shutdown()


