from pycdas.portal.cdas import CDASPortal
import time

try:
    portal = CDASPortal()
    response_manager = portal.createResponseManager()

    # Stage the CDAS app using the "{CDAS_HOME}>> sbt stage" command.
    portal.start_CDAS()

    time.sleep(4)

    datainputs = """[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""

    portal.sendMessage("execute", [ "CDSpark.max", datainputs, ""] )

    response_manager.waitForResponse()

finally:

    portal.shutdown()


