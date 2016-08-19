import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}

object localExecutionTest extends App {
  val serverConfiguration = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )
  val fragment = "t|merra/daily|0,0,0,0|248,42,144,288"
  val varName = fragment.split('|').head

  val async = false
  val t0 = System.nanoTime()
  val service = "cds2"
  val identifier = "CDS.workflow"
  val operation = "CDS.sum"
  val level = 30
  val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString )
  val datainputs_op = "[domain=[{\"name\":\"d1\",\"lev\":{\"start\":%d,\"end\":%d,\"system\":\"indices\"}}],variable=[{\"uri\":\"fragment:/%s\",\"name\":\"%s:v1\",\"domain\":\"d1\"}],operation=[{\"name\":\"%s\",\"input\":\"v1\",\"axes\":\"t\"}]]".format( level, level, operation, fragment, varName )
  val datainputs_anomaly_1D = """[domain=[{"name":"d2","lat":{"start":30.0,"end":30.0,"system":"values"},"lon":{"start":30.0,"end":30.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.anomaly","input":"v1","axes":"t"}]]""".format( level, level, fragment, varName )
  val datainputs_subset_1D =  """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.subset","input":"v1","axes":"t"}]]""".format( level, level, fragment, varName )
  val datainputs_ave_1D =  """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.average","input":"v1","axes":"t"}]]""".format( level, level, fragment, varName )
  val datainputs_subset_0D = """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"},"time":{"start":100,"end":100,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.subset","input":"v1","axes":"t"}]]""".format( level, level, fragment, varName )
  val datainputs_yearly_cycle_1D = """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.timeBin","input":"v1","axes":"t","unit":"month","period":"1","mod":"12"}]]""".format( level, level, fragment, varName )
  val parsed_data_inputs = wpsObjectParser.parseDataInputs( datainputs_yearly_cycle_1D )
  val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
  webProcessManager.logger.info( "Completed request '%s' in %.4f sec".format( identifier, (System.nanoTime()-t0)/1.0E9) )
  webProcessManager.logger.info( response.toString )
  System.exit(0)
}