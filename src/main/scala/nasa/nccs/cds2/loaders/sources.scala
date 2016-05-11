package nasa.nccs.cds2.loaders
import nasa.nccs.cdapi.cdm.Collection

object AxisNames {
  def apply( x: String = "", y: String = "", z: String = "", t: String = "" ): Option[AxisNames] = {
    val nameMap = Map( 'x' -> x, 'y' -> y, 'z' -> z, 't' -> t )
    Some( new AxisNames( nameMap ) )
  }
}
class AxisNames( val nameMap: Map[Char,String]  ) {
  def apply( dimension: Char  ): Option[String] = nameMap.get( dimension ) match {
    case Some(name) => if (name.isEmpty) None else Some(name)
    case None=> throw new Exception( s"Not an axis: $dimension" )
  }
}

object Collections {
  val datasets = Map(
    "merra/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "merra2/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA2/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "cfsr/mon/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "ecmwf/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "merra/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "cfsr/6hr/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/6hr/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "ecmwf/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "merra/mon/atmos/ta" -> Collection( ctype="file", url="file://Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA_TEST_DATA.ta.nc", vars=List( "ta"  )  ),
    "merra2/mon/atmos/ta" -> Collection( ctype="file", url="file://Users/tpmaxwel/Dropbox/Tom/Data/MERRA/ta.MERRA2.nc", vars=List( "ta"  )  ),
    "synth/constant-1" -> Collection( ctype="file", url="file://Users/tpmaxwel/Dropbox/Tom/Data/synth/r0.nc", vars=List( "ta"  )  )
  )
  def toXml(): xml.Elem = {
    <collections> { for( (id,collection) <- datasets ) yield <collection id={id}> {collection.vars.mkString(",")} </collection>} </collections>
  }
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase

  def toXml( collectionId: String ): xml.Elem = {
    datasets.get( collectionId ) match {
      case Some(collection) => <collection id={collectionId}> { collection.vars.mkString(",") } </collection>
      case None => <error> { "Invalid collection id:" + collectionId } </error>
    }
  }
  def parseUri( uri: String ): ( String, String ) = {
    if (uri.isEmpty) ("", "")
    else {
      val recognizedUrlTypes = List("file", "collection")
      val uri_parts = uri.split(":/")
      val url_type = normalize(uri_parts.head)
      if (recognizedUrlTypes.contains(url_type) && (uri_parts.length == 2)) (url_type, uri_parts.last)
      else throw new Exception("Unrecognized uri format: " + uri + ", type = " + uri_parts.head + ", nparts = " + uri_parts.length.toString + ", value = " + uri_parts.last)
    }
  }

  def getCollection(collection_uri: String, var_names: List[String] = List()): Option[Collection] = {
    parseUri(collection_uri) match {
      case (ctype, cpath) => ctype match {
        case "file" => Some(Collection(ctype = "file", url = collection_uri, vars = var_names))
        case "collection" => datasets.get( cpath.stripPrefix("/").toLowerCase )
      }
    }
  }
}


