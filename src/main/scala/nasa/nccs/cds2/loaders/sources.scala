package nasa.nccs.cds2.loaders
import java.io.{File, FileNotFoundException, FileOutputStream}
import java.net.URL
import java.nio.channels.Channels
import java.nio.file.{Files, Path, Paths}

import collection.JavaConverters._
import scala.collection.JavaConversions._
import collection.mutable
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.caching.{FragmentPersistence, collectionDataCache}
import nasa.nccs.cdapi.cdm.{Collection, NCMLWriter}
import nasa.nccs.utilities.Loggable
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2

import scala.concurrent.Future
import scala.xml.XML

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

trait XmlResource extends Loggable {
  val Encoding = "UTF-8"

  def saveXML( fileName: String, node: xml.Node ) = {
    val pp = new xml.PrettyPrinter( 800, 2 )
    val fos = new FileOutputStream(fileName)
    val writer = Channels.newWriter(fos.getChannel(), Encoding)
    try {
      writer.write("<?xml version='1.0' encoding='" + Encoding + "'?>\n")
      writer.write(pp.format(node))
    } finally {
      writer.close()
    }
  }

  def getCacheFilePath( resourcePath: String ): String =
    try {
      getFilePath(resourcePath)
    } catch {
      case ex: Exception =>
        sys.env.get("CDAS_CACHE_DIR") match {
          case Some( cache_path ) => Paths.get( cache_path, resourcePath ).toString
          case None =>
            val home = System.getProperty("user.home")
            Paths.get( home, ".cdas", "cache", resourcePath ).toString
        }

    }

  def getFilePath(resourcePath: String) = Option( getClass.getResource(resourcePath) ) match {
      case None => Option( getClass.getClassLoader.getResource(resourcePath) ) match {
        case None =>
          throw new Exception(s"Resource $resourcePath does not exist!")
        case Some(r) => r.getPath
      }
      case Some(r) => r.getPath
    }

  def attr( node: xml.Node, att_name: String ): String = { node.attribute(att_name) match { case None => ""; case Some(x) => x.toString }}
  def attrOpt( node: xml.Node, att_name: String ): Option[String] = node.attribute(att_name).map( _.toString )
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase
  def nospace( value: String ): String  = value.filter(_!=' ')
}

object Mask  {
  def apply( mtype: String, resource: String ) = { new Mask(mtype,resource) }
}
class Mask( val mtype: String, val resource: String ) extends XmlResource {
  override def toString = "Mask( mtype=%s, resource=%s )".format( mtype, resource )
  def getPath: String = getCacheFilePath( resource )
}

object Masks extends XmlResource {
  val mid_prefix: Char = '#'
  val masks = loadMaskXmlData(getCacheFilePath("/masks.xml"))

  def isMaskId( maskId: String ): Boolean = (maskId(0) == mid_prefix )

  def loadMaskXmlData(filePath:String): Map[String,Mask] = {
    Map(XML.loadFile(filePath).child.flatMap( node => node.attribute("id") match {
      case None => None;
      case Some(id) => Some( (mid_prefix +: id.toString) -> createMask(node)); }
    ) :_* )
  }
  def createMask( n: xml.Node ): Mask = { Mask( attr(n,"mtype"), attr(n,"resource") ) }

  def getMask( id: String ): Option[Mask] = masks.get(id)

  def getMaskIds: Set[String] = masks.keySet
}

object Collections extends XmlResource {
  val maxCapacity: Int=100000
  val initialCapacity: Int=250
  val datasets: ConcurrentLinkedHashMap[String,Collection] =  loadCollectionXmlData( Map( "global" -> getFilePath("/global_collections.xml"), "local" -> getCacheFilePath("/local_collections.xml") ) )

  def toXml: xml.Elem =
    <collections>
      { for( ( id: String, collection:Collection ) <- datasets ) yield collection.toXml }
    </collections>

  def getCollectionMetadata( collId: String  ): List[nc2.Attribute] = {
    findCollection( collId ) match {
      case None => List.empty[nc2.Attribute]
      case Some( coll ) => coll.getDatasetMetadata
    }
  }

  def getVariableListXml(cids: Array[String]): xml.Elem = {
    <collections>
      { for (cid <- cids) yield Collections.findCollection(cid) match {
      case Some(collection) => <variables cid={collection.url}> {collection.vars.map(varName => collectionDataCache.getVariable(collection, varName).toXml)} </variables>
      case None => <error> {"Unknown collection id in identifier: " + cid } </error>
    }}
    </collections>
  }

  def getPersistedVariableListXml: xml.Elem = FragmentPersistence.getFragmentListXml

  def idSet: Set[String] = datasets.keySet.toSet
  def values: Iterator[Collection] = datasets.valuesIterator

  def toXml( scope: String ): xml.Elem = {
    <collections>
      { for( ( id: String, collection:Collection ) <- datasets; if collection.scope.equalsIgnoreCase(scope) ) yield collection.toXml }
    </collections>
  }

  def uriToFile( uri: String ): String = {
    uri.toLowerCase.split(":").last.stripPrefix("/").stripPrefix("/").replaceAll("[-/]","_").replaceAll("[^a-zA-Z0-9_.]", "X") + ".xml"
  }

  def removeCollections( collectionIds: Array[String] ): Array[String] = {
    val removedCids = collectionIds.flatMap( collectionId =>
      findCollection(collectionId: String) match {
        case Some(collection) =>
          logger.info( "Removing collection: " + collectionId )
          datasets.remove(collectionId)
          if (collection.ctype.equals("file")) { collection.ncmlFile.delete() }
          Some(collection.id)
        case None => logger.error("Attempt to delete collection that does not exist: " + collectionId); None
      }
    )
    persistLocalCollections()
    removedCids
  }

  def addCollection( uri: String, path: String, fileFilter: String="", vars: List[String] = List.empty[String] ): Collection = {
    val url = "file:" + NCMLWriter.getCachePath("NCML").resolve( uriToFile(uri) )
    val id = uri.split(":").last.stripPrefix("/").stripPrefix("/").toLowerCase
    val cvars = if(vars.isEmpty) getVariableList( path ) else vars
    val collection = Collection( id, url, path, fileFilter, "local", cvars )
    datasets.put( id, collection  )
    persistLocalCollections()
    collection
  }

  def updateCollection( collection: Collection ): Collection = {
    datasets.put( collection.id, collection  )
    persistLocalCollections()
    collection
  }

  def findNcFile(file: File): Option[File] = {
    file.listFiles.filter( _.isFile ) foreach {
      f =>  if( f.getName.startsWith(".") ) return None
            else if (NCMLWriter.isNcFile(f)) return Some(f)
    }
    file.listFiles.filter( _.isDirectory ) foreach { f => findNcFile(f) match { case Some(f) => return Some(f); case None => Unit } }
    None
  }

  def hasChildNcFile(file: File): Boolean = { findNcFile(file).isDefined }

  def getVariableList( path: String ): List[String] = {
    findNcFile( new File(path) ) match {
      case Some(f) =>
        val dset: NetcdfDataset = NetcdfDataset.openDataset( f.getAbsolutePath )
        dset.getVariables.toList.flatMap( v => if(v.isCoordinateVariable) None else Some(v.getShortName) )
      case None => throw new Exception( "Can't find any nc files in dataset path: " + path )
    }
  }

//  def loadCollectionTextData(url:URL): Map[String,Collection] = {
//    val lines = scala.io.Source.fromURL( url ).getLines
//    val mapItems = for( line <- lines; toks =  line.split(';')) yield
//      nospace(toks(0)) -> Collection( url=nospace(toks(1)), url=nospace(toks(1)), vars=getVarList(toks(3)) )
//    mapItems.toMap
//  }

  def isChild( subDir: String,  parentDir: String ): Boolean = Paths.get( subDir ).toAbsolutePath.startsWith( Paths.get( parentDir ).toAbsolutePath )
  def findCollectionByPath( subDir: String ): Option[Collection] = datasets.values.toList.find { case collection => if( collection.path.isEmpty) { false } else { isChild( subDir, collection.path ) } }

  def loadCollectionXmlData( filePaths: Map[String,String] ): ConcurrentLinkedHashMap[String,Collection] = {
    val maxCapacity: Int=100000
    val initialCapacity: Int=250
    val datasets = new ConcurrentLinkedHashMap.Builder[String, Collection].initialCapacity(initialCapacity).maximumWeightedCapacity(maxCapacity).build()
    for ( ( scope, filePath ) <- filePaths.iterator; if Files.exists( Paths.get(filePath) ) ) {
      try {
        XML.loadFile(filePath).child.foreach( node => node.attribute("id") match {
          case None => None;
          case Some(id) => datasets.put(id.toString.toLowerCase, getCollection(node,scope))
        })
      } catch { case err: java.io.IOException => throw new Exception( "Error opening collection data file {%s}: %s".format( filePath, err.getMessage) ) }
    }
    datasets
  }
  def persistLocalCollections(prettyPrint: Boolean = true) = {
    if(prettyPrint) saveXML( getCacheFilePath("/local_collections.xml"), toXml("local") )
    else XML.save( getCacheFilePath("/local_collections.xml"), toXml("local") )
  }

  def getVarList( var_list_data: String  ): List[String] = var_list_data.filter(!List(' ','(',')').contains(_)).split(',').toList
  def getCollection( n: xml.Node, scope: String ): Collection = { Collection( attr(n,"id"), attr(n,"url"), attr(n,"path"), attr(n,"fileFilter"), scope, n.text.split(",").toList )}

  def findCollection( collectionId: String ): Option[Collection] = Option( datasets.get( collectionId ) )

  def getCollectionXml( collectionId: String ): xml.Elem = {
    Option( datasets.get( collectionId ) ) match {
      case Some( collection: Collection ) => collection.toXml
      case None => <error> { "Invalid collection id:" + collectionId } </error>
    }
  }
  def parseUri( uri: String ): ( String, String ) = {
    if (uri.isEmpty) ("", "")
    else {
      val uri_parts = uri.split(":/")
      val url_type = normalize(uri_parts.head)
      if(uri_parts.length == 2) (url_type, uri_parts.last)
      else throw new Exception("Unrecognized uri format: " + uri + ", type = " + uri_parts.head + ", nparts = " + uri_parts.length.toString + ", value = " + uri_parts.last)
    }
  }

//  def getCollection(collection_uri: String, var_names: List[String] = List()): Option[Collection] = {
//    parseUri(collection_uri) match {
//      case (ctype, cpath) => ctype match {
//        case "file" => Some(Collection( url = collection_uri, vars = var_names ))
//        case "collection" =>
//          val collection_key = cpath.stripPrefix("/").stripSuffix("\"").toLowerCase
//          logger.info( " getCollection( %s ) ".format(collection_key) )
//          datasets.get( collection_key )
//      }
//    }
//  }

  def getCollectionKeys(): Array[String] = datasets.keys.toArray
}






