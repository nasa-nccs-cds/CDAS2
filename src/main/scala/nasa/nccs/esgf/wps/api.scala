package nasa.nccs.esgf.wps

package servers
import nasa.nccs.cds2.utilities.appParameters
import nasa.nccs.utilities.Loggable

class APIManager( serverConfiguration: Map[String,String] ) extends Loggable {

  val providers = Map( "cdas" -> cds2ServiceProvider )
  val default_service = cds2ServiceProvider
  appParameters.addConfigParams( serverConfiguration )

  def getServiceProvider(service: String = ""): ServiceProvider = {
    providers.getOrElse(service,default_service)
  }
}
