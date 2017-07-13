from pycdas.portal.cdas import *
import time, sys, cdms2, os
import pandas as pd
from pycdas.portal.cdas import *
import matplotlib.pyplot as plt

startServer = False
portal = None
request_port = 5670
response_port = 5671
host = "cldra"
server = "localhost"

try:

    portal = CDASPortal(ConnectionMode.CONNECT, server, request_port, response_port)
    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = '[domain=[{"name":"d0","lat":{"start":5,"end":40,"system":"values"},"lon":{"start":80,"end":120,"system":"values"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, server, datainputs ); sys.stdout.flush()

    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, '{ "response":"file" }'] )
    responses = response_manager.getResponseVariables(rId)
    timeSeries = responses[0](squeeze=1)
    timeSeries -= 273.15


except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


