package nasa.nccs.cds2.portal
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.cdas.portal.CDASPortal
import nasa.nccs.cdas.portal.CDASPortal.ConnectionMode._
import nasa.nccs.utilities.Loggable

object CDASapp {
  def elem( array: Array[String], index: Int, default: String = "" ): String = if( array.length > index ) array(index) else default
}

class CDASapp( mode: CDASPortal.ConnectionMode, request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends CDASPortal( mode, request_port, response_port ) {
  import CDASapp._
  val processManager = new ProcessManager( appConfiguration )
  val process = "cdas"
  Runtime.getRuntime().addShutdownHook( new Thread() { override def run() { term() } } )


  override def postArray(header: String, data: Array[Byte]) = {

  }

  override def execUtility(utilSpec: Array[String]) = {

  }

  def getResult( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResult( process, elem(resultSpec,1) )
    sendResponse( result.toString )
  }

  def getResultStatus( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResultStatus( process, elem(resultSpec,1) )
    sendResponse( result.toString )
  }

  override def execute( taskSpec: Array[String] ) = {
    val process_name = elem(taskSpec,1)
    val datainputs = if( taskSpec.length > 2 ) wpsObjectParser.parseDataInputs( taskSpec(2) ) else Map.empty[String, Seq[Map[String, Any]]]
    val runargs = if( taskSpec.length > 3 ) wpsObjectParser.parseMap( taskSpec(3) ) else Map.empty[String, Any]
    val response = processManager.executeProcess( process, process_name, datainputs, runargs.mapValues(_.toString) ).toString
    sendResponse( response )
  }

  override def getCapabilities(utilSpec: Array[String]) = {
    val result: xml.Elem = processManager.getCapabilities( process, elem(utilSpec,1) )
    sendResponse( result.toString )
  }

  override def describeProcess(procSpec: Array[String]) = {
    val result: xml.Elem = processManager.describeProcess( process, elem(procSpec,1) )
    sendResponse( result.toString  )
  }
}

object CDASApplication extends App with Loggable {
  import CDASapp._
  logger.info( "Executing CDAS with args: " + args.mkString(",") )
  val connect_mode = elem(args,0,"bind")
  val request_port = elem(args,1,"0").toInt
  val response_port = elem(args,2,"0").toInt
  val appConfiguration =  Map.empty[String,String]
  val cmode = if( connect_mode.toLowerCase.startsWith("c") ) CONNECT else BIND
  val app = new CDASapp( cmode, request_port, response_port, appConfiguration )
  app.run()
}
