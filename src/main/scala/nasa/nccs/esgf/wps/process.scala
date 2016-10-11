package nasa.nccs.esgf.wps

import nasa.nccs.esgf.wps.servers.APIManager
import scala.collection.mutable
import scala.collection.immutable
import scala.xml._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class NotAcceptableException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

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
        serviceProvider.describeWPSProcess( name )
      case None =>
        throw new NotAcceptableException("Unrecognized service: " + service)
    }
  }

  def getCapabilities(service: String, identifier: String): xml.Elem = {
    apiManager.getServiceProvider(service) match {
      case Some(serviceProvider) =>
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
        serviceProvider.getWPSCapabilities( identifier )
      case None =>
        throw new NotAcceptableException("Unrecognized service: " + service)
    }
  }

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

