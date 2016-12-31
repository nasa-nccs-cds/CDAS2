package nasa.nccs.cds2.portal
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.cdas.portal.CDASPortal
import nasa.nccs.utilities.Loggable

class CDASapp( request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends CDASPortal( request_port, response_port ) {
  val processManager = new ProcessManager( appConfiguration )
  val process = "cdas"

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
    logger.info( "Processing execute request: " + taskSpec.mkString(", "))
    val process_name = taskSpec(1)
    val datainputs = wpsObjectParser.parseDataInputs( taskSpec(2) )
    val runargs = if( taskSpec.length > 3 ) wpsObjectParser.parseMap( taskSpec(3) ) else Map.empty[String, Any]
    logger.info( "Executing cdas process: " + process_name )
    val response = processManager.executeProcess( process, process_name, datainputs, runargs.mapValues(_.toString) ).toString
    logger.info( "Sending execute response: " + response )
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
  val request_port = args(0).toInt
  val response_port = args(1).toInt
  val appConfiguration =  Map.empty[String,String]
  val app = new CDASapp( request_port, response_port, appConfiguration )
  app.run()
}
