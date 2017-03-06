package nasa.nccs.cdas.portal
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.cdas.portal.CDASPortal.ConnectionMode._
import nasa.nccs.utilities.Loggable

object CDASapp {
  def elem( array: Array[String], index: Int, default: String = "" ): String = if( array.length > index ) array(index) else default
}

class CDASapp( mode: CDASPortal.ConnectionMode, request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends CDASPortal( mode, request_port, response_port ) {
  import CDASapp._
  val processManager = new ProcessManager( appConfiguration )
  val process = "cdas"
  val printer = new scala.xml.PrettyPrinter(200, 3)
  Runtime.getRuntime().addShutdownHook( new Thread() { override def run() { term() } } )


  override def postArray(header: String, data: Array[Byte]) = {

  }

  override def execUtility(utilSpec: Array[String]) = {

  }

  def getResult( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResult( process, resultSpec(0) )
    sendResponse( resultSpec(0), printer.format( result )  )
  }

  def getResultStatus( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResultStatus( process, resultSpec(0) )
    sendResponse( resultSpec(0), printer.format( result )  )
  }

  override def execute( taskSpec: Array[String] ) = {
    val process_name = elem(taskSpec,2)
    val datainputs = if( taskSpec.length > 3 ) wpsObjectParser.parseDataInputs( taskSpec(3) ) else Map.empty[String, Seq[Map[String, Any]]]
    val runargs = if( taskSpec.length > 4 ) wpsObjectParser.parseMap( taskSpec(4) ) else Map.empty[String, Any]
    val response = processManager.executeProcess( process, process_name, datainputs, runargs.mapValues(_.toString) )
    sendResponse( taskSpec(0), printer.format( response ) )
  }

  override def getCapabilities(utilSpec: Array[String]) = {
    val result: xml.Elem = processManager.getCapabilities( process, elem(utilSpec,2) )
    sendResponse( utilSpec(0), printer.format( result ) )
  }

  override def describeProcess(procSpec: Array[String]) = {
    val result: xml.Elem = processManager.describeProcess( process, elem(procSpec,2) )
    sendResponse( procSpec(0), printer.format( result )  )
  }
}

object CDASApplication extends App with Loggable {
  import CDASapp._
  logger.info( s"Executing CDAS with args: ${args.mkString(",")}, nprocs: ${Runtime.getRuntime.availableProcessors()}" )
  val connect_mode = elem(args,0,"bind")
  val request_port = elem(args,1,"0").toInt
  val response_port = elem(args,2,"0").toInt
  val appConfiguration =  Map.empty[String,String]
  val cmode = if( connect_mode.toLowerCase.startsWith("c") ) CONNECT else BIND
  val app = new CDASapp( cmode, request_port, response_port, appConfiguration )
  app.run()
}
