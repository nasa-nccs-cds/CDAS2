from pycdas.portal.cdas import *
import time, sys, cdms2, vcs, os, EzTemplate

# assumes the java/scala side of the CDASPortal has been started using the startupCDASPortal.py script.


request_port = 5670
response_port = 5671
cdas_server = "10.71.9.11"

try:
    portal = CDASPortal( ConnectionMode.CONNECT, cdas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":70,"end":90,"system":"values"},"lon":{"start":5,"end":45,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/cdas/cache/collections/NCML/CIP_MERRA2_6hr_tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId2 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    objectResponses = response_manager.getResponseVariables(rId2)

    x = vcs.init()
    objVar  = objectResponses[0](squeeze=1)
    M = EzTemplate.Multi(rows=1, columns=1)

    t2 = M.get(legend='Ave Surface Temp Plot')
    plot2 = x.createboxfill("tave_plot")
    x.plot(objVar, t2, plot2)

    png_file = os.path.expanduser(  '~/plot1.png' )
    x.png(png_file)

finally:
    portal.shutdown()


