from pycdas.portal.cdas import *
import time, sys

startServer = False
portal = None
request_port = 4356
response_port = 4357

try:

    if startServer:
        portal = CDASPortal()
        portal.start_CDAS()
        time.sleep(20)
    else:
        portal = CDASPortal(ConnectionMode.CONNECT, "localhost", request_port, response_port)

    response_manager = portal.createResponseManager()

    t0 = time.time()
#    datainputs = """[domain=[{"name":"d0","time":{"start":'2013-01-16',"end":'2015-12-16',"system":"values"}}],variable=[{"uri":"file:///Users/tpmaxwel/Dropbox/Tom/Data/MERRA/atmos_ua.nc","name":"ua:v1","domain":"d0"}],operation=[{"name":""python.cdmsExt.zaDemo"","input":"v1","domain":"d0"}]]"""
    datainputs = """[domain=[{"name":"d0","filter":"DJF"}],variable=[{"uri":"file:///Users/tpmaxwel/Dropbox/Tom/Data/MERRA/atmos_ua.nc","name":"ua:v1","domain":"d0"}],operation=[{"name":""python.cdmsExt.zaDemo"","input":"v1","domain":"d0"}]]"""
    print "Sending request on port {0}: {1}".format( portal.request_port, datainputs ); sys.stdout.flush()
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}".format( time.time()-t0 ); sys.stdout.flush()

except Exception, err:
    print "Exception occurred: " + err

finally:

    portal.shutdown()


