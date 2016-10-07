package nasa.nccs.cds2.utilities
import java.nio.file.Paths
import nasa.nccs.utilities.Loggable
import scala.io.Source


object appParameters extends Loggable {

  val cacheDir = getCacheDirectory
  val parmFile = Paths.get( cacheDir, "cdas.properties" ).toString
  private var _map: Map[String,String]  = getParameterMap

  def apply( key: String, default: String ): String = _map.getOrElse( key, default )

  def apply( key: String ): String = _map.get( key ) match {
    case Some( value ) => value
    case None => throw new Exception( "Missing required parameter in appParameters file(%s): %s".format( parmFile, key ) )
  }

  def bool( key: String, default: Boolean ): Boolean = _map.get( key ) match {
    case Some( value ) => value.toLowerCase.trim.startsWith("t")
    case None => default
  }

  def addConfigParams( configuration: Map[String,String] ) = { _map = _map ++ configuration }

  def keySet: Set[String] = _map.keySet

  def getCacheDirectory: String = {
    sys.env.get("CDAS_CACHE_DIR") match {
      case Some(cache_path) => cache_path
      case None =>
        val home = System.getProperty("user.home")
        Paths.get(home, ".cdas", "cache" ).toString
    }
  }

  def getParameterMap: Map[String,String] = {
    logger.info( "Loading parameters from parm file: " + parmFile )
    val items = Source.fromFile( parmFile ).getLines.map( _.split("=") ).flatMap(
      toks => if( toks.length == 2 ) Some( toks(0).trim -> toks(1).trim ) else None
    )
    Map( items.toSeq: _* )
  }
}


