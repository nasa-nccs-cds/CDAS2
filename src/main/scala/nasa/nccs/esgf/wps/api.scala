package nasa.nccs.esgf.wps

package servers
import nasa.nccs.cds2.utilities.appParameters
import org.slf4j.LoggerFactory

class APIManager( serverConfiguration: Map[String,String] ) {

  val providers = Map( "cds2" -> cds2ServiceProvider )
  val default_service = cds2ServiceProvider
  val logger = LoggerFactory.getLogger(classOf[APIManager])
  appParameters.addConfigParams( serverConfiguration )

  def getServiceProvider(service: String = ""): ServiceProvider = {
    providers.getOrElse(service,default_service)
  }
}
