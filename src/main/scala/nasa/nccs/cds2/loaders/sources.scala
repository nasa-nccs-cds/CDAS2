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
  val datasets = loadCollectionData

  def toXml(): xml.Elem = {
    <collections> { for( (id,collection) <- datasets ) yield <collection id={id}> {collection.vars.mkString(",")} </collection>} </collections>
  }
  def loadCollectionData: Map[String,Collection] = {
    val stream : java.io.InputStream = getClass.getResourceAsStream("/collections.txt")
    val lines = scala.io.Source.fromInputStream( stream ).getLines
    val mapItems = for( line <- lines; toks =  line.split(';')) yield ( toks(0).filter(_!=' ') -> Collection( ctype=toks(1).filter(_!=' '), url=toks(2).filter(_!=' '), vars=getVarList(toks(3))  ) )
    mapItems.toMap
  }
  def getVarList( var_list_data: String  ): List[String] = var_list_data.filter(!List(' ','(',')').contains(_)).split(',').toList

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

object TestCollections extends App {
  println( Collections.datasets )
}


