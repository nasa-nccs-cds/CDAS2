from pycdas.portal.cdas import *

request_port = 5670
response_port = 5671
cdas_server = "localhost"

try:
    portal = CDASPortal( ConnectionMode.CONNECT, cdas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","time":{"start":0,"end":3000,"system":"indices"},"lat":{"start":70,"end":90,"system":"values"},"lon":{"start":25,"end":45,"system":"values"}}],variable=[{"uri":"file:////Users/tpmaxwel/.cdas/cache/collections/NCML/MERRA2-6hr-ana_Np.200001.ncml","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","cycle":"diurnal","bin":"month","axes":"t"}]]"""
    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)

    print " Received " + str( len(responses) ) + " responses "

finally:
    portal.shutdown()


