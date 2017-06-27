package nasa.nccs.esgf.wps
import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.cdas.portal.CDASPortal.ConnectionMode
import nasa.nccs.cdas.portal.CDASPortalClient
import nasa.nccs.utilities.Loggable

import scala.collection.JavaConversions._
import scala.collection.JavaConversions._
import scala.xml

class NotAcceptableException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

trait GenericProcessManager {
  def describeProcess(service: String, name: String): xml.Node;
  def getCapabilities(service: String, identifier: String): xml.Node;
  def executeProcess(service: String, process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Node
  def getResultFilePath( service: String, resultId: String ): Option[String]
  def getResult( service: String, resultId: String ): xml.Node
  def getResultStatus( service: String, resultId: String ): xml.Node
}



class ProcessManager( serverConfiguration: Map[String,String] ) extends GenericProcessManager with Loggable {
  def apiManager = new APIManager( serverConfiguration )

  def unacceptable(msg: String): Unit = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def shutdown(service: String) = {
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.shutdown()
  }

  def describeProcess(service: String, name: String): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
    //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.describeWPSProcess( name )
  }

  def getCapabilities(service: String, identifier: String): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.getWPSCapabilities( identifier )
  }

  def executeProcess(service: String, process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
    logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.executeProcess(process_name, datainputs, runargs)
  }

  def getResultFilePath( service: String, resultId: String ): Option[String] = {
    logger.info( "CDAS ProcessManager-> getResultFile: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultFilePath(resultId)
  }

  def getResult( service: String, resultId: String ): xml.Node = {
    logger.info( "CDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResult(resultId)
  }

  def getResultVariable( service: String, resultId: String ): Option[RDDTransientVariable] = {
    logger.info( "CDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultVariable(resultId)
  }

  def getResultStatus( service: String, resultId: String ): xml.Node = {
    logger.info( "CDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultStatus(resultId)
  }
}

class zmqProcessManager( serverConfiguration: Map[String,String] )  extends GenericProcessManager with Loggable {
  val portal = new CDASPortalClient( ConnectionMode.BIND, "localhost", 5670, 5671 )
  val response_manager = portal.createResponseManager()

  def unacceptable(msg: String) = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def describeProcess(service: String, name: String): xml.Node  =  {
    val rId = portal.sendMessage( "describeProcess", List( name ).toArray )
    val responses = response_manager.getResponses(rId,true).toList
    scala.xml.XML.loadString( responses(0) )
  }

  def getCapabilities(service: String, identifier: String): xml.Node = {
    val rId = portal.sendMessage( "getCapabilities", List( "" ).toArray )
    val responses = response_manager.getResponses(rId,true).toList
    scala.xml.XML.loadString( responses(0) )
  }

  def executeProcess(service: String, process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Node = {
    throw new Exception("Not yet supported!")
//    val rId = portal.sendMessage( "execute", List( process_name, datainputs, runargs ).toArray )
//    val responses: List[String] = response_manager.getResponses(rId,true).toList
  }

  def getResultFilePath( service: String, resultId: String ): Option[String] = {
    throw new Exception("Not yet supported!")
  }

  def getResult( service: String, resultId: String ): xml.Node = {
    val responses = response_manager.getResponses(resultId,true).toList
    scala.xml.XML.loadString( responses(0) )
  }

  def getResultStatus( service: String, resultId: String ): xml.Node = {
    throw new Exception("Not yet supported!")
  }
}


