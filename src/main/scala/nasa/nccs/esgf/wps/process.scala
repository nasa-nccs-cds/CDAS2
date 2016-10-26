package nasa.nccs.esgf.wps
import nasa.nccs.esgf.wps.servers.APIManager
import nasa.nccs.utilities.Loggable


class NotAcceptableException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

class ProcessManager( serverConfiguration: Map[String,String] ) extends Loggable {
  def apiManager = new APIManager( serverConfiguration )

  def unacceptable(msg: String): Unit = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
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
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
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
}

