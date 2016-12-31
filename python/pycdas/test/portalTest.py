from pycdas.portal.cdas import CDASPortal
import time

portal = CDASPortal()
portal.start_CDAS()

time.sleep(4)

datainputs="""[variable=[{"domain":"d0","uri":"collection:/GISS_r1i1p1","id":"tas|v0"}];domain=[{"id":"d0"}];operation=[{"input":["v0"],"name":"CDSpark.max","axes":"t"}]]"""

portal.sendMessage("execute", [ "CDSpark.max", datainputs, ""] )

portal.join()
