from pycdas.portal.cdas import *
import time, sys, cdms2, vcs, os, EzTemplate
import xml.etree.ElementTree

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
    datainputs = '[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"t"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, server, datainputs ); sys.stdout.flush()

    rId1 = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, '{ "response":"file" }'] )
    fileResponses = response_manager.getResponseVariables(rId1)

    rId2 = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, '{ "response":"object" }'] )
    objectResponses = response_manager.getResponseVariables(rId2)

    x = vcs.init()
    fileVar = fileResponses[0](squeeze=1)
    objVar  = objectResponses[0](squeeze=1)
    M = EzTemplate.Multi(rows=2, columns=1)

    t1 = M.get(legend='File Var Plot')
    plot1 = x.createboxfill("plot1")
    x.plot(fileVar, t1, plot1)

    t2 = M.get(legend='Object Var Plot')
    plot2 = x.createboxfill("plot2")
    x.plot(objVar, t2, plot2)

    png_file = os.path.expanduser(  '~/plot1.png' )
    x.png(png_file)



except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


