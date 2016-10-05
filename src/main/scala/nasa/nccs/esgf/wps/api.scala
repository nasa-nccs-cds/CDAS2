package nasa.nccs.esgf.wps

package servers
import org.slf4j.LoggerFactory

class APIManager( serverConfiguration: Map[String,String] ) {

  val providers = Map( "cds2" -> new cds2ServiceProvider(serverConfiguration) )
  val default_service = "esgf"
  val logger = LoggerFactory.getLogger(classOf[APIManager])

  def getServiceProvider(service: String = ""): Option[ServiceProvider] = {
    def actual_service = if (service == "") default_service else service
    //    logger.info( " Executing WPS service " +  service )
    providers.get(actual_service)
  }
}
