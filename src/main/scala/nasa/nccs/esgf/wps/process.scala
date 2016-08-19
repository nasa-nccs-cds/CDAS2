package nasa.nccs.esgf.wps

import nasa.nccs.esgf.wps.servers.APIManager
import scala.collection.mutable
import scala.collection.immutable
import scala.xml._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class NotAcceptableException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

/*class ProcessInput(val name: String, val itype: String, val maxoccurs: Int, val minoccurs: Int) {

  def toXml = {
    <input id={ name } type={ itype } maxoccurs={ maxoccurs.toString } minoccurs={ minoccurs.toString }/>
  }
}

class Process(val name: String, val description: String, val inputs: List[ProcessInput]) {

  def toXml =
    <process id={ name }>
      <description id={ description }> </description>
      <inputs>
        { inputs.map(_.toXml ) }
      </inputs>
    </process>

  def toXmlHeader =
    <process id={ name }> <description> { description } </description> </process>
}

class ProcessList(val process_list: List[Process]) {

  def toXml =
    <processes>
      { process_list.map(_.toXml ) }
    </processes>

  def toXmlHeaders =
    <processes>
      { process_list.map(_.toXmlHeader ) }
    </processes>
}*/

class ProcessManager( serverConfiguration: Map[String,String] ) {
  val logger = LoggerFactory.getLogger(this.getClass)
  def apiManager = new APIManager( serverConfiguration )

  def printLoggerInfo = {
    import ch.qos.logback.classic.LoggerContext
    import ch.qos.logback.core.util.StatusPrinter
    StatusPrinter.print( LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext] )
  }

  def unacceptable(msg: String): Unit = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def describeProcess(service: String, name: String): xml.Elem = {
    apiManager.getServiceProvider(service) match {
      case Some(serviceProvider) =>
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
        serviceProvider.describeProcess( name )
      case None =>
        throw new NotAcceptableException("Unrecognized service: " + service)
    }
  }

  def getCapabilities(service: String, identifier: String): xml.Elem = {
    apiManager.getServiceProvider(service) match {
      case Some(serviceProvider) =>
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
        serviceProvider.getCapabilities( identifier )
      case None =>
        throw new NotAcceptableException("Unrecognized service: " + service)
    }
  }

  //  def listProcesses(service: String): xml.Elem = {
  //    apiManager.getServiceProvider(service) match {
  //      case Some(serviceProvider) =>
  //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
  //        serviceProvider.listProcesses()
  //      case None =>
  //        throw new NotAcceptableException("Unrecognized service: " + service)
  //    }
  //  }

  def executeProcess(service: String, process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem = {
    apiManager.getServiceProvider(service) match {
      case Some(serviceProvider) =>
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
        serviceProvider.executeProcess(process_name, datainputs, runargs)
      case None => throw new NotAcceptableException("Unrecognized service: " + service)
    }
  }

  def getResultFilePath( service: String, resultId: String ): Option[String] = {
    apiManager.getServiceProvider(service) match {
      case Some(serviceProvider) => serviceProvider.getResultFilePath(resultId)
      case None => throw new NotAcceptableException("Unrecognized service: " + service)
    }
  }
}


/*
import org.scalatest.FunSuite

class ParserTest extends FunSuite {

  test("DescribeProcess") {
    println(webProcessManager.listProcesses())
    val process = webProcessManager.describeProcess("CWT.Sum")
    process match {
      case Some(p) => println(p)
      case None => println("Unrecognized process")
    }
    assert(true)
  }

}
*/

object wpsExecutionTest extends App {
  val serverConfiguration = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )

  val async = false
  val t0 = System.nanoTime()
  val service = "cds2"
  val identifier = "CDS.workflow"
  val operation = "CDS.sum"
  val level = 30
  val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString )
  val datainputs_op = "[domain=[{\"name\":\"d1\",\"lev\":{\"start\":%d,\"end\":%d,\"system\":\"indices\"}}],variable=[{\"uri\":\"fragment:/t|merra/daily|0,0,0,0|248,42,144,288\",\"name\":\"t:v1\",\"domain\":\"d1\"}],operation=[{\"name\":\"%s\",\"input\":\"v1\",\"axes\":\"t\"}]]".format( level, level, operation )
  val datainputs_anomaly_1D = """[domain=[{"name":"d2","lat":{"start":30.0,"end":30.0,"system":"values"},"lon":{"start":30.0,"end":30.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/t|merra/daily|0,0,0,0|248,42,144,288","name":"t:v1","domain":"d2"}],operation=[{"name":"CDS.anomaly","input":"v1","axes":"t"}]]""".format( level, level )
  val datainputs_subset_1D =  """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/t|merra/daily|0,0,0,0|248,42,144,288","name":"t:v1","domain":"d2"}],operation=[{"name":"CDS.subset","input":"v1","axes":"t"}]]""".format( level, level )
  val datainputs_ave_1D =  """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/t|merra/daily|0,0,0,0|248,42,144,288","name":"t:v1","domain":"d2"}],operation=[{"name":"CDS.average","input":"v1","axes":"t"}]]""".format( level, level )
  val datainputs_subset_0D = """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":0,"end":0,"system":"indices"},"time":{"start":100,"end":100,"system":"indices"}}],variable=[{"uri":"fragment:/t|merra/daily|0,0,0,0|248,42,144,288","name":"t:v1","domain":"d2"}],operation=[{"name":"CDS.subset","input":"v1","axes":"t"}]]"""
  val datainputs_yearly_cycle_1D = """[domain=[{"name":"d2","lat":{"start":20.0,"end":20.0,"system":"values"},"lon":{"start":20.0,"end":20.0,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/t|merra/daily|0,0,0,0|248,42,144,288","name":"t:v1","domain":"d2"}],operation=[{"name":"CDS.timeBin","input":"v1","axes":"t","unit":"month","period":"1","mod":"12"}]]""".format( level, level )
  val parsed_data_inputs = wpsObjectParser.parseDataInputs( datainputs_yearly_cycle_1D )
  val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
  webProcessManager.logger.info( "Completed request '%s' in %.4f sec".format( identifier, (System.nanoTime()-t0)/1.0E9) )
  webProcessManager.logger.info( response.toString )
  System.exit(0)
}

object assortTest extends App {
 println( true.toString )
}

