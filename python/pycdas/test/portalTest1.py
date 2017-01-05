from pycdas.portal.cdas import *
import time

# assumes the java/scala side of the CDASPortal has been started using the startupCDASPortal.py script.

request_port = 4356
response_port = 4357

portal = CDASPortal( ConnectionMode.CONNECT, "localhost", request_port, response_port )

datainputs = """[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""

portal.sendMessage("execute", [ "CDSpark.max", datainputs, ""] )

response = portal.waitForResponse()

print "Received response: " + response


