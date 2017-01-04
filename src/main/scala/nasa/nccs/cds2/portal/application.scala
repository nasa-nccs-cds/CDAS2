package nasa.nccs.cds2.portal
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.cdas.portal.CDASPortal
import nasa.nccs.cdas.portal.CDASPortal.ConnectionMode._
import nasa.nccs.utilities.Loggable

class CDASapp( mode: CDASPortal.ConnectionMode, request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends CDASPortal( mode, request_port, response_port ) {
  val processManager = new ProcessManager( appConfiguration )
  val process = "cdas"
  Runtime.getRuntime().addShutdownHook( new Thread() { override def run() { term() } } )

  override def postArray(header: String, data: Array[Byte]) = {

  }

  override def execUtility(utilSpec: Array[String]) = {

  }

  def getResult( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResult( process, resultSpec(1) )
    sendResponse( result.toString )
  }

  def getResultStatus( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResultStatus( process, resultSpec(1) )
    sendResponse( result.toString )
  }

  override def execute( taskSpec: Array[String] ) = {
    val process_name = taskSpec(1)
    val datainputs = wpsObjectParser.parseDataInputs( taskSpec(2) )
    val runargs = if( taskSpec.length > 3 ) wpsObjectParser.parseMap( taskSpec(3) ) else Map.empty[String, Any]
    val response = processManager.executeProcess( process, process_name, datainputs, runargs.mapValues(_.toString) ).toString
    sendResponse( response )
  }

  override def getCapabilities(utilSpec: Array[String]) = {
    val result: xml.Elem = processManager.getCapabilities( process, utilSpec(1) )
    sendResponse( result.toString )
  }

  override def describeProcess(procSpec: Array[String]) = {
    val result: xml.Elem = processManager.describeProcess( process, procSpec(1) )
    sendResponse( result.toString  )
  }
}

object CDASApplication extends App with Loggable {
  logger.info( "Executing CDAS with args: " + args.mkString(",") )
  val connect_mode = if( args.length > 0 ) args(0) else "bind"
  val request_port = if( args.length > 1 ) args(1).toInt else 0
  val response_port = if( args.length > 2 ) args(2).toInt else 0
  val appConfiguration =  Map.empty[String,String]
  val cmode = if( connect_mode.toLowerCase.startsWith("c") ) CONNECT else BIND
  val app = new CDASapp( cmode, request_port, response_port, appConfiguration )
  app.run()
}
