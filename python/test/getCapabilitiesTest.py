from pycdas.portal.cdas import CDASPortal
import time

portal = CDASPortal()
portal.start_CDAS()

time.sleep(4)

portal.sendMessage("getCapabilities", [ "" ] )

portal.waitForResponse()

portal.shutdown()
