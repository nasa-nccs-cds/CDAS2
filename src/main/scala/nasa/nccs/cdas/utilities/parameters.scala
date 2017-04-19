package nasa.nccs.cdas.utilities
import java.nio.file.{Files, Paths}

import nasa.nccs.utilities.Loggable

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object appParameters extends Serializable with Loggable {

  private var _map: Map[String,String] = Map.empty[String, String]
  val cacheDir = getCacheDirectory
  val parmFile = Paths.get( cacheDir, "cdas.properties" ).toString
  buildParameterMap

  def apply( key: String, default: String ): String = _map.getOrElse( key, default )
  def getParameterMap(): Map[String,String] = _map

  def apply( key: String ): Option[String] = _map.get( key );

  def bool( key: String, default: Boolean ): Boolean = _map.get( key ) match {
    case Some( value ) => value.toLowerCase.trim.startsWith("t")
    case None => default
  }

  def addConfigParams( configuration: Map[String,String] ) = { _map = _map ++ configuration }

  def keySet: Set[String] = _map.keySet

  def getCacheDirectory: String = _map.getOrElse( "cdas.cache.dir", {
    sys.env.get("CDAS_CACHE_DIR") match {
      case Some(cache_path) => cache_path
      case None =>
        val home = System.getProperty("user.home")
        Paths.get(home, ".cdas", "cache" ).toString
    }
  })

  def buildParameterMap(): Unit =
    if( Files.exists( Paths.get(parmFile) ) ) {
      val params: Iterator[Array[String]] = for ( line <- Source.fromFile(parmFile).getLines() ) yield { line.split('=') }
      _map = _map ++ Map( params.filter( _.length > 1 ).map( a => a.head.trim->a.last.trim ).toSeq: _* )
    }
    else { logger.warn("Can't find default parameter file: " + parmFile); }


//  def getParameterMap: Map[String,String] = {
//    logger.info( "Loading parameters from parm file: " + parmFile )
//    val items = Source.fromFile( parmFile ).getLines.map( _.split("=") ).flatMap(
//      toks => if( toks.length == 2 ) Some( toks(0).trim -> toks(1).trim ) else None
//    )
//    Map( items.toSeq: _* )
//  }
}


