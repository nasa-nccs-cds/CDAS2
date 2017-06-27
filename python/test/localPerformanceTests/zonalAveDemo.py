from pycdas.portal.cdas import *
import time, sys

startServer = False
portal = None
request_port = 5670
response_port = 5671
host = "cldra"
server = "localhost"

if host == "webmap":
    dataset = "file:/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/merra_mon_ua.xml"
    var = "ua"
elif host == "cldra":
    dataset = "file:/home/tpmaxwel/.cdas/cache/collections/NCML/CIP_MERRA_ua_mon.ncml"
    var = "ua"
else:
    dataset = "http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc"
    var = "tas"

cldraGISSdset = "file:///home/tpmaxwel/.cdas/cache/collections/NCML/GISS_E2H_r1i1p1.ncml"
localGISSdset = "file:///Users/tpmaxwel/.cdas/cache/collections/NCML/giss_r1i1p1.xml"
currentGISSDataset = localGISSdset

try:

    if startServer:
        portal = CDASPortal()
        portal.start_CDAS()
        time.sleep(20)
    else:
        portal = CDASPortal(ConnectionMode.CONNECT, server, request_port, response_port)

    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = '[domain=[{"name":"d0","lat":{"start":30,"end":40,"system":"indices"},"lon":{"start":30,"end":40,"system":"indices"},"lev":{"start":10,"end":10,"system":"indices"},"time":{"start":0,"end":100,"system":"indices"}}],variable=[{"uri":"' + currentGISSDataset + '","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","axes":"xt","filter":"DJF"}]]'
#    datainputs = '[domain=[{"name":"d0","lat":{"start":30,"end":40,"system":"indices"},"lon":{"start":30,"end":40,"system":"indices"},"lev":{"start":10,"end":10,"system":"indices"},"time":{"start":0,"end":100,"system":"indices"}}],variable=[{"uri":"file:///home/tpmaxwel/.cdas/cache/collections/NCML/GISS_E2H_r1i1p1.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","axes":"xy"}]]'
#    datainputs = '[domain=[{"name":"d0"}],variable=[{"uri":"' + dataset + '","name":"ua:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.ave","input":"v1","axes":"xt","filter":"DJF"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, server, datainputs ); sys.stdout.flush()
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}".format( time.time()-t0 ); sys.stdout.flush()
    print "Responses = " + str(responses)

except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


