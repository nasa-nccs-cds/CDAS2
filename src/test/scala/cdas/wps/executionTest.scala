package cdas.wps

import java.nio.file.Paths
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import org.scalatest._
import scala.io.Source
import org.scalatest.Tag

// Execution Example (tag = yearly_cycle):
// >> sbt test-only *wpsSuite -- -n yearly_cycle
// Define frag id in ~/.cdas/test_config.txt
// sample test_config.txt:
// fragment=t|merra/daily|0,0,0,0|248,42,144,288

class wpsSuite extends LocalExecutionTestSuite {
  val fragment = getConfigValue("fragment")
  val varName = fragment.split('|').head
  val collection =  fragment.split('|')(1)
  val level = 0
  val lat = 50f
  val lon = 20f

  test("op") {
    val datainputs = "[domain=[{\"name\":\"d1\",\"lev\":{\"start\":%d,\"end\":%d,\"system\":\"indices\"}}],variable=[{\"uri\":\"fragment:/%s\",\"name\":\"%s:v1\",\"domain\":\"d1\"}],operation=[{\"name\":\"%s\",\"input\":\"v1\",\"axes\":\"t\"}]]".format(level, level, operation, fragment, varName)
    executeTest(datainputs)
  }
  test("anomaly_1D", Tag("anomaly")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.anomaly","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, varName)
    executeTest(datainputs)
  }
  test("subset_1D", Tag("subset")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.subset","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, varName)
    executeTest(datainputs)
  }
  test("subset_1D_cache", Tag("subset+cache")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}},{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDS.subset","input":"v1","domain":"d2"}]]""".format(lat, lat, lon, lon, level, level, collection, varName)
    executeTest(datainputs)
  }
  test("average_1D", Tag("average")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.average","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, varName)
    executeTest(datainputs)
  }
  test("subset_0D") {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"},"time":{"start":100,"end":100,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.subset","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, varName)
    executeTest(datainputs)
  }
  test("yearly_cycle_1D", Tag("yearly_cycle")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDS.timeBin","input":"v1","axes":"t","unit":"month","period":"1","mod":"12"}]]""".format(lat, lat, lon, lon, level, level, fragment, varName)
    val response = executeTest(datainputs)
  }
  test("createV", Tag("createV")) {
    val datainputs = """ [domain=[{"name":"r0","longitude":{"start":-125.875,"end":-125.875,"system":"values"},"latitude":{"start":-3.7604263305664176,"end":-3.7604263305664176,"system":"values"},"level":{"start":100000,"end":100000,"system":"values"}},{"name":"r1","time":{"start":"2010-01-16T12:00:00","end":"2010-01-16T12:00:00","system":"values"}}];variable={"uri":"fragment:/%s","name":"%s:v0","domain":"r0"};operation=[{"name":"CDS.anomaly","input":"v0","axes":"t"},{"name":"CDS.timeBin","input":"v0","axes":"t","bins":"t|month|ave|year"},{"name":"CDS.subset","input":"v0","domain":"r1"}]]""".format( fragment, varName)
    val response = executeTest(datainputs)
  }
}

class LocalExecutionTestSuite extends FunSuite with Matchers {
  val serverConfiguration = Map[String,String]()
  val configMap = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )
  val service = "cds2"
  val identifier = "CDS.workflow"
  val operation = "CDS.sum"
  val config_file_path = Paths.get(  System.getProperty("user.home"), ".cdas", "test_config.txt" ).toString
  lazy val config = getConfiguration

  def executeTest( datainputs: String, async: Boolean = false ): xml.Elem = {
    val t0 = System.nanoTime()
    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString )
    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
    val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
    webProcessManager.logger.info("Completed request '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    webProcessManager.logger.info(response.toString)
    response
  }

  def getConfigValue(key: String, defaultVal: Option[String] = None): String = {
    configMap.get(key) match {
      case Some(value) => value.toString
      case None => config.get(key) match {
          case Some(value) => value
          case None => defaultVal match {
            case Some(dval) => dval
            case None => throw new Exception("Config file '" + config_file_path + "' is missing required config value: " + key)
          }
        }
    }
  }

  def getConfiguration: Map[String,String] = {
    try {
      val tuples = Source.fromFile(config_file_path).getLines.map(line => line.split('=')).toList
      try {
        Map[String, String](tuples.map(t => (t(0).trim -> t(1).trim)): _*)
      } catch {
        case err: ArrayIndexOutOfBoundsException => throw new Exception("Format error in config file: missing '='")
      }
    } catch {
      case  ex: java.io.FileNotFoundException => throw new Exception("Must create test config file: " + config_file_path )
    }
  }
}
