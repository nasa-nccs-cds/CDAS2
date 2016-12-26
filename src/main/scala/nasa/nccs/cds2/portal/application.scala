package nasa.nccs.cds2.portal
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.cdas.portal.CDASPortal

class CDASapp( request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends CDASPortal( request_port, response_port ) {
  val processManager = new ProcessManager( appConfiguration )

  def postArray(header: String, data: Array[Byte]) = {}
  def execUtility(utilSpec: Array[String]) = {}

  def execute( taskSpec: Array[String] ) = {
    val datainputs = wpsObjectParser.parseDataInputs( taskSpec(1) )
  }

  def getCapabilities(utilSpec: Array[String]) = {
    val result: xml.Elem = processManager.getCapabilities( "cdas", utilSpec(0) )
    sendResponse( result.toString );
  }

  def describeProcess(procSpec: Array[String]) = {
    val result: xml.Elem = processManager.describeProcess( "cdas", procSpec(0) )
    sendResponse( result.toString  );
  }

}
