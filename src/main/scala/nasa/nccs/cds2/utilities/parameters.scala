package nasa.nccs.cds2.utilities
import java.nio.file.Paths
import scala.io.Source


object appParameters {

  val cacheDir = getCacheDirectory
  private val _map: Map[String,String]  = getParameterMap

  def apply( key: String, default: String ): String = _map.getOrElse(key,default)
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
    val parmFile = Paths.get( cacheDir, "cdas.properties" ).toString
    val items = Source.fromFile( parmFile ).getLines.map( _.split("=") ).flatMap(
      toks => if( toks.length == 2 ) Some( toks(0).trim -> toks(1).trim ) else None
    )
    Map( items.toSeq: _* )
  }
}


