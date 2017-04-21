from pycdas.portal.cdas import *
import time, sys

startServer = False
portal = None
request_port = 4356
response_port = 4357
server="cldradn102.dassvm.nccs.nasa.gov"

try:

    if startServer:
        portal = CDASPortal()
        portal.start_CDAS()
        time.sleep(20)
    else:
        portal = CDASPortal(ConnectionMode.CONNECT, server, request_port, response_port)

    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = '[domain=[{"name":"d0"}],variable=[{"uri":"file:/home/tpmaxwel/.cdas/cache/cdscan/merra_mon_ua.xml","name":"ua:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.ave","input":"v1","axes":"xt","filter":"DJF"}]]'
#    datainputs = '[domain=[{"name":"d0"}],variable=[{"uri":"file:/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/merra_mon_ua.xml","name":"ua:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.wave","input":"v1","axes":"xt","filter":"DJF"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, server, datainputs ); sys.stdout.flush()
    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, ""] )
    responses = response_manager.getResponses(rId)
    print "Completed OP in time {0}".format( time.time()-t0 ); sys.stdout.flush()

except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


