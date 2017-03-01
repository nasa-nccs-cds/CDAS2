package nasa.nccs.cdapi.cdm

import java.nio.channels.{FileChannel, NonReadableChannelException, ReadableByteChannel}

import nasa.nccs.caching.collectionDataCache
import ucar.{ma2, nc2}
import java.nio.file.{Files, Path, Paths}
import java.io.{FileWriter, _}
import java.nio._
import java.util.Formatter

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.cdapi.tensors.{CDDoubleArray, CDFloatArray}
import nasa.nccs.cdas.loaders.XmlResource
import nasa.nccs.cdas.utilities.appParameters
import nasa.nccs.utilities.{Loggable, cdsutils}
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset._
import ucar.ma2.DataType
import ucar.nc2.constants.CDM

import scala.collection.{concurrent, mutable}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.xml.XML
import org.apache.commons.io.IOUtils
import nasa.nccs.cdas.loaders.Collections
import nasa.nccs.esgf.process.DataSource
import ucar.nc2._
import ucar.nc2.ncml.NcMLReader

import scala.concurrent.Future

object Collection extends Loggable {
  def apply( id: String,  dataPath: String, fileFilter: String = "", scope: String="", title: String= "", vars: List[String] = List() ) = {
    val ctype = dataPath match {
      case url if(url.startsWith("http:")) => "dap"
      case url if(url.startsWith("file:")) => "file"
      case dpath if(dpath.toLowerCase.endsWith(".csv")) => "csv"
      case fpath if(new File(fpath).isFile) => "file"
      case dir if(new File(dir).isDirectory) => "file"
      case _ => throw new Exception( "Unrecognized Collection type, dataPath = " + dataPath )
    }
    new Collection( ctype, id, dataPath, fileFilter, scope, title, vars )
  }

  def aggregate( dsource: DataSource ): xml.Elem = {
    val col = dsource.collection
    logger.info( "XXXX-> Creating collection '" + col.id + "' path: " + col.dataPath )
//    val url = if ( col.dataPath.startsWith("http:") ) {
//      col.dataPath
//    } else {
//      col.createNCML()
//    }
    col.generateAggregation()
  }

  def aggregate( colId: String, path: File ): xml.Elem = {
    val col = Collection(colId, path.getAbsolutePath)
    col.generateAggregation()
  }
}

object CDGrid extends Loggable {
  def apply(name: String, datfilePath: String): CDGrid = {
    val gridFilePath: String = NCMLWriter.getCachePath("NCML").resolve(Collections.idToFile(name, ".nc")).toString
    if( !Files.exists( Paths.get(gridFilePath) ) ) { createGridFile(gridFilePath, datfilePath) }
    CDGrid.create(name, gridFilePath)
  }

  def create(name: String, gridFilePath: String): CDGrid = {
    val gridDS = NetcdfDataset.acquireDataset(gridFilePath, null)
    try {
      val coordSystems: List[CoordinateSystem] = gridDS.getCoordinateSystems.toList
      val dset_attributes: List[nc2.Attribute] = gridDS.getGlobalAttributes.map(a => {
        new nc2.Attribute(name + "--" + a.getFullName, a)
      }).toList

      for (variable <- gridDS.getVariables; if variable.isCoordinateVariable) {
        variable match {
          case cvar: VariableDS => gridDS.addCoordinateAxis(variable.asInstanceOf[VariableDS])
        }
      }
      val coordAxes: List[CoordinateAxis] = gridDS.getCoordinateAxes.toList
      val dimensions = gridDS.getDimensions.toList
      val conv = gridDS.getConventionUsed
      val title = gridDS.getTitle
      new CDGrid(name, gridFilePath, coordAxes, coordSystems, dimensions, dset_attributes)
    } finally {
      gridDS.close()
    }
  }

  def isInt( s: String ): Boolean = try { s.toInt; true } catch { case err: Exception => false }

  def getDimensionNames( dimIDs: Iterable[String], dimNames: Iterable[String] ): Iterable[String] =
    dimIDs flatMap ( id => if( isInt(id) || id.equals("*") ) Some(id) else dimNames.find( _ equals id) match {
      case Some( dname ) => Some(dname)
      case None => dimNames.find( _.split(':')(0) equals id) match {
        case Some( dname ) => Some(dname)
        case None => None
      }
    })

  def getNewGroup( groupMap: mutable.Map[String,nc2.Group], oldGroup: Group, gridWriter: NetcdfFileWriter ): Group = {
    val gname = if(oldGroup==null) "" else oldGroup.getShortName
    if( gname.isEmpty ) gridWriter.addGroup(null,"root") else {
      groupMap.getOrElseUpdate( gname, gridWriter.addGroup( getNewGroup( groupMap, oldGroup.getParentGroup, gridWriter ), gname ) )
    }
  }

  def createGridFile(gridFilePath: String, datfilePath: String) = {
    logger.info( s"Creating #grid# file $gridFilePath from datfilePath: $datfilePath" )
    val ncDataset: NetcdfDataset = NetcdfDataset.acquireDataset(datfilePath, null)
    val gridWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, gridFilePath, null)
    val dimMap = Map(ncDataset.getDimensions.map(d => NCMLWriter.getName(d) -> gridWriter.addDimension(null, NCMLWriter.getName(d), d.getLength)): _*)
    val groupMap = mutable.HashMap.empty[String,nc2.Group]
    val varTups = for (cvar <- ncDataset.getVariables) yield {
      val dataType = cvar match {
        case coordAxis: CoordinateAxis => if(coordAxis.getAxisType == AxisType.Time) DataType.LONG else cvar.getDataType
        case x => cvar.getDataType
      }
      val oldGroup = cvar.getGroup
      val newGroup = getNewGroup( groupMap, oldGroup, gridWriter )
      val newVar = gridWriter.addVariable( newGroup, NCMLWriter.getName(cvar), dataType, getDimensionNames( cvar.getDimensionsString.split(' '), dimMap.keys ).mkString(" ")  )
//      val newVar = gridWriter.addVariable( newGroup, NCMLWriter.getName(cvar), dataType, cvar.getDimensionsString  )
      NCMLWriter.getName(cvar) -> (cvar -> newVar)
    }
    val varMap = Map(varTups.toList: _*)
    for ( (cvar, newVar) <- varMap.values; attr <- cvar.getAttributes ) cvar match  {
      case coordAxis: CoordinateAxis =>
        if( (coordAxis.getAxisType == AxisType.Time) &&  attr.getShortName.equalsIgnoreCase(CDM.UNITS) ) {
          gridWriter.addVariableAttribute(newVar, new Attribute( CDM.UNITS, cdsutils.baseTimeUnits ) )
        } else {
          gridWriter.addVariableAttribute(newVar, attr)
        }
      case x =>
        gridWriter.addVariableAttribute(newVar, attr)
    }
    val globalAttrs = Map(ncDataset.getGlobalAttributes.map(attr => attr.getShortName -> attr): _*)
    globalAttrs.mapValues(attr => gridWriter.addGroupAttribute(null, attr))
    gridWriter.create()
    for ((cvar, newVar) <- varMap.values; if cvar.isCoordinateVariable && (cvar.getRank == 1) ) cvar match  {
      case coordAxis: CoordinateAxis => if( coordAxis.getAxisType == AxisType.Time ) {
        val ( time_values, bounds ) = FileHeader.getTimeValues( ncDataset, coordAxis )
        newVar.addAttribute( new Attribute( CDM.UNITS, cdsutils.baseTimeUnits ) )
        gridWriter.write( newVar, ma2.Array.factory( ma2.DataType.LONG, coordAxis.getShape, time_values ) )
        varMap.get(coordAxis.getBoundaryRef) match {
          case Some( ( cvarBnds, newVarBnds )  ) => gridWriter.write( newVarBnds, ma2.Array.factory( ma2.DataType.LONG, cvarBnds.getShape, bounds ) )
          case None => Unit
        }
      } else {
        gridWriter.write(newVar, coordAxis.read())
        coordAxis match {
          case coordAxis1D: CoordinateAxis1D =>
            varMap.get(coordAxis1D.getBoundaryRef) match {
              case Some( ( cvarBnds, newVarBnds )  ) =>
                val bounds: Array[Double] = ((0 until coordAxis1D.getShape(0)) map ( index => coordAxis1D.getCoordBounds(index) )).toArray.flatten
                gridWriter.write( newVarBnds, ma2.Array.factory( ma2.DataType.DOUBLE, cvarBnds.getShape, bounds ) )
              case None => Unit
            }
          case x => Unit
        }
      }
      case x => Unit
    }
//    for ( ( bndsvar, cvar ) <- boundsSpecs.flatten )  varMap.get(bndsvar) match {
//      case Some((bvar, newVar)) =>
//        cvar match  {
//          case coordAxis: CoordinateAxis => if( coordAxis.getAxisType == AxisType.Time ) {
//            bvar match  {
//              case dsvar: VariableDS =>
//                val time_values = dsvar.read()
//                val units = dsvar.getUnitsString()
//                newVar.addAttribute( new Attribute( CDM.UNITS, cdsutils.baseTimeUnits ) )
//                gridWriter.write( newVar, ma2.Array.factory( ma2.DataType.DOUBLE, dsvar.getShape, time_values ) )
//              case x =>
//                gridWriter.write(newVar, bvar.read())
//            }
//          } else {
//            gridWriter.write(newVar, bvar.read())
//          }
//          case x => gridWriter.write(newVar, bvar.read())
//        }
//      case None => Unit
//    }
    gridWriter.close()
  }
}

class CDGrid( val name: String,  val gridFilePath: String, val coordAxes: List[CoordinateAxis], val coordSystems: List[CoordinateSystem], val dimensions: List[Dimension], val attributes: List[nc2.Attribute] ) extends Loggable {

  def gridFileExists(): Boolean = try {
    val file = new File(gridFilePath)
    val path = file.toPath()
    ( Files.exists(path) && Files.isRegularFile(path) )
  } catch { case err: Exception => false }

  def deleteAggregation = if( gridFileExists ) new File(gridFilePath).delete()
  override def toString = gridFilePath
  def getCoordinateAxes: List[CoordinateAxis] = coordAxes

  def getGridSpec: String = "file:/" + gridFilePath
  def getGridFile: String = "file:/" + gridFilePath

  def findCoordinateAxis(name: String): Option[CoordinateAxis] = {
    val gridDS = NetcdfDataset.acquireDataset(gridFilePath, null)
    try {
      val axisOpt = Option( gridDS.findCoordinateAxis( name ) )
      axisOpt.map( axis => {
          axis.setCaching(true);
          axis.read()
          axis
        }
      )
    } catch {
      case err: Exception =>
        logger.error("Can't find Coordinate Axis " + name + " in gridFile " + gridFilePath + " , error = " + err.toString );
        logger.error(err.getStackTrace.mkString("\n"))
        None
    } finally { gridDS.close() }
  }

  def getTimeCoordinateAxis: Option[CoordinateAxis1DTime] = {
    val gridDS = NetcdfDataset.acquireDataset(gridFilePath, null)
    try {
      val axisOpt = Option( gridDS.findCoordinateAxis( AxisType.Time ) )
      axisOpt.map( axis => {
        axis.setCaching(true);
        axis.read()
        CoordinateAxis1DTime.factory( gridDS, axis, new Formatter() )
      })
    } catch {
      case err: Exception =>
        logger.error("Can't create time Coordinate Axis for collection " + name + " in gridFile " + gridFilePath + ", error = " + err.toString );
        logger.error(err.getStackTrace.mkString("\n"))
        None
    } finally { gridDS.close() }
  }


  def findCoordinateAxis( atype: AxisType ): Option[CoordinateAxis] = {
    val gridDS = NetcdfDataset.acquireDataset(gridFilePath, null)
    try {
      Option( gridDS.findCoordinateAxis( atype ) ).map( axis => {
        axis.setCaching(true);
        axis.read()
        axis
      } )
    } catch {
      case err: Exception =>
        logger.error("Can't find Coordinate Axis with type: " + atype.toString + " in gridFile " + gridFilePath + ", error = " + err.toString  );
        logger.error(err.getStackTrace.mkString("\n"))
        None
    } finally { gridDS.close() }
  }

  def getVariable( varShortName: String ): Option[nc2.Variable] = {
    val ncDataset: NetcdfDataset = NetcdfDataset.acquireDataset( gridFilePath, null )
    try {
      ncDataset.getVariables.toList.find ( v => (v.getShortName equals varShortName) )
    } catch {
      case err: Exception => logger.error("Can't get Variable " + varShortName + " from gridFile " + gridFilePath ); throw err;
    } finally { ncDataset.close() }
  }

  def getAttribute( keyValuePair: (String, Option[String] )  ): Option[nc2.Attribute] = keyValuePair._2 match {
    case Some( value ) => if(value.isEmpty) None else Some( new nc2.Attribute( keyValuePair._1, value ) )
    case None => None
  }

  def getVariableMetadata( varShortName: String ): List[nc2.Attribute] = {
    val ncDataset: NetcdfDataset = NetcdfDataset.acquireDataset( gridFilePath, null )
    try {
      getVariable( varShortName ) match {
        case Some( ncVariable ) =>
          val attributes = ncVariable.getAttributes.toList
          val keyValuePairs = List(
            "description" -> ncVariable.getDescription,
            "units" -> ncVariable.getUnitsString,
            "dtype" -> ncVariable.getDataType.toString,
            "dims" -> ncVariable.getDimensionsString,
            "shape" -> ncVariable.getShape.mkString(","),
            "fullname" -> ncVariable.getFullName
          ) map { case (key,value) => getAttribute(key, Option(value)) }
          attributes ++ keyValuePairs.flatten
        case None => throw new Exception("Can't find variable %s in collection %s".format(varShortName,name) )
      }
    } catch {
      case err: Exception => logger.error("Can't get Variable metadata for var: " + varShortName + " in gridFile " + gridFilePath ); throw err;
    } finally { ncDataset.close() }
  }
}

class Collection( val ctype: String, val id: String, val uri: String, val fileFilter: String = "", val scope: String="local", val title: String= "", val vars: List[String] = List() ) extends Serializable with Loggable {
  val collId = Collections.idToFile(id)
  val dataPath = getDataFilePath(uri,collId)
  val variables = new ConcurrentLinkedHashMap.Builder[String, CDSVariable].initialCapacity(10).maximumWeightedCapacity(500).build()
  override def toString = "Collection( id=%s, ctype=%s, path=%s, title=%s, fileFilter=%s )".format(id, ctype, dataPath, title, fileFilter)
  def isEmpty = dataPath.isEmpty
  lazy val varNames = vars.map(varStr => varStr.split(Array(':', '|')).head)
  val grid = CDGrid(id, dataPath)

  def deleteAggregation() = grid.deleteAggregation
  def getVariableMetadata(varName: String): List[nc2.Attribute] = grid.getVariableMetadata(varName)
  def getGridFilePath = grid.gridFilePath
  def getVariable(varName: String): CDSVariable = variables.getOrElseUpdate(varName, new CDSVariable(varName, this))

  def getDatasetMetadata(): List[nc2.Attribute] = List(
      new nc2.Attribute("variables", varNames),
      new nc2.Attribute("path", dataPath),
      new nc2.Attribute("ctype", ctype)
    ) ++ grid.attributes

  def generateAggregation(): xml.Elem = {
    val ncDataset: NetcdfDataset = NetcdfDataset.acquireDataset(grid.gridFilePath, null)
    try {
      _aggCollection(ncDataset)
    } catch {
      case err: Exception => logger.error("Can't aggregate collection for dataset " + ncDataset.toString); throw err
    } finally {
      ncDataset.close()
    }
  }

  def readVariableData(varShortName: String, section: ma2.Section): ma2.Array = {
    val ncDataset: NetcdfDataset = NetcdfDataset.acquireDataset( dataPath, true, null )
    ncDataset.getVariables.toList.find( v => v.getShortName equals varShortName ) match {
      case Some(variable) =>
        try {
          variable.read(section)
        } catch {
          case err: Exception =>
            logger.error("Can't read data for variable %s in dataset %s due to error: %s".format(varShortName, ncDataset.getLocation, err.toString));
            logger.error("Variable shape: (%s),  section: { o:(%s) s:(%s) }".format(variable.getShape.mkString(","), section.getOrigin.mkString(","), section.getShape.mkString(",")));
            logger.error(err.getStackTrace.map(_.toString).mkString("\n"))
            throw err
        } finally {
          ncDataset.close()
        }
      case None => throw new Exception( s"Can't find variable $varShortName in dataset $dataPath ")
    }
  }

  private def _aggCollection(dataset: NetcdfDataset): xml.Elem = {
    val vars = dataset.getVariables.filter(!_.isCoordinateVariable).map(v => Collections.getVariableString(v)).toList
    val title: String = Collections.findAttribute(dataset, List("Title", "LongName"))
    val newCollection = new Collection(ctype, id, dataPath, fileFilter, scope, title, vars)
    Collections.updateCollection(newCollection)
    newCollection.toXml
  }
  def url(varName: String = "") = ctype match {
    case "http" => dataPath
    case _ => "file:/" + dataPath
  }

  def toXml: xml.Elem = {
    val varData = vars.mkString(";")
    if (fileFilter.isEmpty) {
      <collection id={id} ctype={ctype} grid={grid.toString} path={dataPath} title={title}>
        {varData}
      </collection>
    } else {
      <collection id={id} ctype={ctype} grid={grid.toString} path={dataPath} fileFilter={fileFilter} title={title}>
        {varData}
      </collection>
    }
  }

  def createNCML( pathFile: File, collectionId: String  ): String = {
    val _ncmlFile = NCMLWriter.getCachePath("NCML").resolve(collectionId).toFile
    val recreate = appParameters.bool("ncml.recreate", false)
    if (!_ncmlFile.exists || recreate) {
      logger.info( s"Creating NCML file for collection ${collectionId} from path ${pathFile.toString}")
      _ncmlFile.getParentFile.mkdirs
      val ncmlWriter = NCMLWriter(pathFile)
      ncmlWriter.writeNCML(_ncmlFile)
    }
    _ncmlFile.toURI.toString
  }

  def getDataFilePath( uri: String, collectionId: String ) : String = ctype match {
    case "csv" =>
      val pathFile: File = new File(toFilePath(uri))
      createNCML( pathFile, collectionId )
    case "file" =>
      val pathFile: File = new File(toFilePath(uri))
      if( pathFile.isDirectory ) createNCML( pathFile, collectionId )
      else pathFile.toURI.toString
    case "dap" => uri
    case _ => throw new Exception( "Unexpected attempt to create Collection data file from ctype " + ctype )
  }

  def toFilePath(path: String): String = {
    if (path.startsWith("file:")) path.substring(5)
    else path
  }
}

object DiskCacheFileMgr extends XmlResource {
  val diskCacheMap = loadDiskCacheMap

  def getDiskCacheFilePath( cachetype: String, cache_file: String ): String =
    if (cache_file.startsWith("/")) {cache_file} else {
      val cacheFilePath = Paths.get( appParameters.cacheDir, cachetype, cache_file )
      Files.createDirectories( cacheFilePath.getParent )
      cacheFilePath.toString
    }



  protected def getDiskCache( id: String = "main" ) = diskCacheMap.get(id) match {
    case None => throw new Exception( "No disk cache defined: " + id )
    case Some( diskCache ) =>
      diskCache.replaceFirst("^~",System.getProperty("user.home"))
  }

  protected def loadDiskCacheMap: Map[String,String] = {
    try {
      var filePath = getFilePath("/cache.xml")
      val tuples = XML.loadFile(filePath).child.map(
        node => node.attribute("id") match {
          case None => None;
          case Some(id) => node.attribute("path") match {
            case Some(path) => Some(id.toString -> path.toString)
            case None => None
          }
        })
      Map(tuples.flatten: _*)
    } catch {
      case err: Throwable => Map( "main"->"~/.cdas2/cache" )
    }
  }
}

trait DiskCachable extends XmlResource {

  def getCacheType: String

  def sizeof[T]( value: T ) = value match {
    case _: Float => 4; case _: Short => 2; case _: Double => 8; case _: Int => 4; case _: Byte => 1
    case x => throw new Exception("Unsupported type in sizeof: " + x.toString)
  }

//  protected def bufferToDiskFloat( data: FloatBuffer  ): String = {
//    val memsize = data.capacity() * 4
//    val cache_file = "a" + System.nanoTime.toHexString
//    try {
//      val t0 = System.nanoTime()
//      val cache_file_path = DiskCacheFileMgr.getDiskCacheFilePath(getCacheType, cache_file)
//      val channel = new RandomAccessFile( cache_file_path, "rw" ).getChannel()
//      val buffer: MappedByteBuffer = channel.map( FileChannel.MapMode.READ_WRITE, 0, memsize )
//      buffer.asFloatBuffer.put(data)
//      channel.close
//      val t1 = System.nanoTime()
//      logger.info( s"Persisted cache data to file '%s', memsize = %d, time = %.2f".format( cache_file_path, memsize, (t1-t0)/1.0E9))
//      cache_file
//    } catch {
//      case err: Throwable => logError(err, s"Error writing data to disk, size = $memsize" ); ""
//    }
//  }

  protected def objectToDisk[T <: Serializable]( record: T  ): String = {
    val cache_file = "c" + System.nanoTime.toHexString
    val ostr = new ObjectOutputStream ( new FileOutputStream( DiskCacheFileMgr.getDiskCacheFilePath( getCacheType, cache_file) ) )
    ostr.writeObject( record )
    cache_file
  }

  protected def objectFromDisk[T <: Serializable]( cache_file: String  ): T = {
    val istr = new ObjectInputStream ( new FileInputStream( DiskCacheFileMgr.getDiskCacheFilePath( getCacheType, cache_file) ) )
    istr.readObject.asInstanceOf[T]
  }

  def getReadBuffer( cache_id: String ): ( FileChannel, MappedByteBuffer ) = {
    val channel = new FileInputStream(DiskCacheFileMgr.getDiskCacheFilePath(getCacheType, cache_id)).getChannel
    channel -> channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size)
  }

  protected def bufferFromDiskFloat( cache_id: String, size: Int  ): Option[FloatBuffer] = {
    try {
      val t0 = System.nanoTime()
      getReadBuffer(cache_id) match { case ( channel, buffer ) =>
        val data: FloatBuffer = buffer.asFloatBuffer
        channel.close
        val t1 = System.nanoTime()
        logger.info( s"Restored persisted data from cache file '%s', memsize = %d, time = %.2f".format( DiskCacheFileMgr.getDiskCacheFilePath(getCacheType, cache_id), size, (t1-t0)/1.0E9))
        Some(data)
      }
    } catch { case err: Throwable => logError(err, s"Error-1 retreiving persisted cache data for cache_id '$cache_id'"); None }
  }

  protected def arrayFromDiskByte( cache_id: String  ): Option[ByteBuffer] = {
    try { getReadBuffer(cache_id) match { case ( channel, buffer ) =>
        channel.close
        Some(buffer)
      }
    } catch { case err: Throwable => logError(err,s"Error-2 retreiving persisted cache data for cache_id '$cache_id'"); None }
  }

}

//object CDSDataset extends DiskCachable  {
//  val cacheType = "dataset"
//  def getCacheType: String = CDSDataset.cacheType
//
////  def load( collection: Collection, varName: String ): CDSDataset = {
////    collection.generateAggregation()
////    load( collection.dataPath, collection, varName )
////  }
//
////  def load( dsetName: String, collection: Collection, varName: String ): CDSDataset = {
////    val t0 = System.nanoTime
////    val uri = collection.url(varName)
////    val rv = new CDSDataset( dsetName, collection, varName  )
////    val t1 = System.nanoTime
////    logger.info( "loadDataset(%s)T> %.4f,  ".format( uri, (t1-t0)/1.0E9 ) )
////    rv
////  }
//
//  def toFilePath( path: String ): String = {
//    if( path.startsWith( "file:/") ) path.substring(6)
//    else path
//  }
//
////  private def loadNetCDFDataSet(dataPath: String): NetcdfDataset = {
////    NetcdfDataset.setUseNaNs(false)
//////    NcMLReader.setDebugFlags( new DebugFlagsImpl( "NcML/debugURL NcML/debugXML NcML/showParsedXML NcML/debugCmd NcML/debugOpen NcML/debugConstruct NcML/debugAggDetail" ) )
////    try {
////      logger.info("Opening NetCDF dataset(2) %s".format(dataPath))
////      NetcdfDataset.openDataset( toFilePath(dataPath), true, null )
////    } catch {
////      case e: java.io.IOException =>
////        logger.error("Couldn't open dataset %s".format(dataPath))
////        throw e
////      case ex: Exception =>
////        logger.error("Something went wrong while reading %s".format(dataPath))
////        throw ex
////    }
////  }
//}
//public class NcMLReader {
//  static private final Namespace ncNS = thredds.client.catalog.Catalog.ncmlNS;
//  static private org.apache.log4j.Logger log = Logger.getLogger(NcMLReader.class);
//
//  private static boolean debugURL = false, debugXML = false, showParsedXML = false;
//  private static boolean debugOpen = false, debugConstruct = false, debugCmd = false;
//  private static boolean debugAggDetail = false;
//
//  static public void setDebugFlags(ucar.nc2.util.DebugFlags debugFlag) {
//    debugURL = debugFlag.isSet("NcML/debugURL");
//    debugXML = debugFlag.isSet("NcML/debugXML");
//    showParsedXML = debugFlag.isSet("NcML/showParsedXML");
//    debugCmd = debugFlag.isSet("NcML/debugCmd");
//    debugOpen = debugFlag.isSet("NcML/debugOpen");
//    debugConstruct = debugFlag.isSet("NcML/debugConstruct");
//    debugAggDetail = debugFlag.isSet("NcML/debugAggDetail");
//  }

//class CDSDataset( val name: String, val collection: Collection ) extends Serializable {
//  val logger = Logger.getLogger(this.getClass)
//  val fileHeaders: Option[DatasetFileHeaders] = getDatasetFileHeaders
//  def getFilePath = collection.dataPath
//
//  def getDatasetFileHeaders: Option[DatasetFileHeaders] = {
//    if( collection.dataPath.startsWith("http:" ) ) { None }
//    else if( collection.dataPath.endsWith(".ncml" ) ) {
//      val aggregation = XML.loadFile(getFilePath) \ "aggregation"
//      val aggDim = (aggregation \ "@dimName").text
//      val fileNodes = ( aggregation \ "netcdf" ).map( node => new FileHeader(  (node \ "@location").text,  (node \ "@coordValue").text.split(",").map( _.trim.toDouble ), false  ) )
//      Some( new DatasetFileHeaders( aggDim, fileNodes ) )
//    } else {
//      None
//    }
//  }
//
//
//
//  def findCoordinateAxis( fullName: String ): Option[CoordinateAxis] = collection.grid.findCoordinateAxis( fullName )
//
////  def getCoordinateAxis( axisType: DomainAxis.Type.Value ): Option[CoordinateAxis] = {
////    axisType match {
////      case DomainAxis.Type.X => Option( coordSystem.getXaxis )
////      case DomainAxis.Type.Y => Option( coordSystem.getYaxis )
////      case DomainAxis.Type.Z => Option( coordSystem.getHeightAxis )
////      case DomainAxis.Type.Lon => Option( coordSystem.getLonAxis )
////      case DomainAxis.Type.Lat => Option( coordSystem.getLatAxis )
////      case DomainAxis.Type.Lev => Option( coordSystem.getPressureAxis )
////      case DomainAxis.Type.T => Option( coordSystem.getTaxis )
////    }
////  }
////
////  def getCoordinateAxis(axisType: Char): CoordinateAxis = {
////    axisType.toLower match {
////      case 'x' => if (coordSystem.isGeoXY) coordSystem.getXaxis else coordSystem.getLonAxis
////      case 'y' => if (coordSystem.isGeoXY) coordSystem.getYaxis else coordSystem.getLatAxis
////      case 'z' =>
////        if (coordSystem.containsAxisType(AxisType.Pressure)) coordSystem.getPressureAxis
////        else if (coordSystem.containsAxisType(AxisType.Height)) coordSystem.getHeightAxis else coordSystem.getZaxis
////      case 't' => coordSystem.getTaxis
////      case x => throw new Exception("Can't recognize axis type '%c'".format(x))
////    }
////  }
//}
//
//// var.findDimensionIndex(java.lang.String name)

object TestType {
  val Buffer = 0
  val Stream = 1
  val Channel = 2
  val Map = 3
  val NcFile = 4
}

class ncReadTest extends Loggable {

  import nasa.nccs.cdas.utilities.runtime
  import java.nio.channels.FileChannel
  import java.nio.file.StandardOpenOption._
  import TestType._

  val url = "file:/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/NCML/merra_daily_2005.xml"
//  val outputFile = "/Users/tpmaxwel/.cdas/cache/test/testBinaryFile.out"
  val outputFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testBinaryFile.out"
//  val outputNcFile = "/Users/tpmaxwel/.cdas/cache/test/testFile.nc"
  val outputNcFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testFile.nc"
  val bufferSize: Int = -1
  val varName = "t"
  val shape = getShape(url, varName)

  val testPlan = Array( Buffer, Map, Buffer, Map )

  executePlan( testPlan )


  def executePlan( exePlan: Array[Int] ) = exePlan.foreach( ttype => execute( ttype ) )

  def execute( testType: Int ) = {
    testType match {
      case TestType.Buffer =>
        val t0 = System.nanoTime()
//        logger.info(s"Reading  $outputFile...")
        val size = shape.foldLeft(1)(_ * _)
        val bSize = size * 4
        val file: File = new File(outputFile);
        val fSize = file.length.toInt
//        logger.info("Reading Float buffer, bSize = %d, shape = (%s): %d elems (%d bytes), file size: %d, (%d floats)".format(bSize, shape.mkString(","), size, size * 4, fSize, fSize / 4))
        val buffer: Array[Byte] = Array.ofDim[Byte](fSize)
        val inputStream = new BufferedInputStream(new FileInputStream(file))
        IOUtils.read(inputStream, buffer)
        val t1 = System.nanoTime()
        val fltBuffer = ByteBuffer.wrap(buffer).asFloatBuffer
//        logger.info("Read Float buffer, capacity = %d".format(fltBuffer.capacity()))
        val data = new CDFloatArray(shape, fltBuffer, Float.MaxValue)
        val sum = CDFloatArray( data.section( Array(0,10,100,100), Array(shape(0),1,1,1) ) ).sum(Array(0))
        val t2 = System.nanoTime()
        logger.info(s"Sum of BUFFER data chunk, size= %.2f M, result shape= %s, Time-{ read: %.2f,  compute: %.2f, total: %.2f,  }, value = %.3f".format(bSize / 1.0E6, sum.getShape.mkString(","), (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t2 - t0) / 1.0E9, sum.getFlatValue(0) ))
      case TestType.Map =>
        val t0 = System.nanoTime()
//        logger.info(s"Reading  $outputFile...")
        val file: File = new File(outputFile)
        val bSize = file.length.toInt
        val fileChannel: FileChannel = new RandomAccessFile(file, "r").getChannel()
        val buffer: MappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size())
        val fltBuffer = buffer.asFloatBuffer
 //       logger.info("Read Float buffer, capacity = %d, shape = (%s): %d elems".format(fltBuffer.capacity(), shape.mkString(","), shape.foldLeft(1)(_ * _)))
        val data = new CDFloatArray(shape, fltBuffer, Float.MaxValue)
        val sum = CDFloatArray( data.section( Array(0,10,100,100), Array(shape(0),1,1,1) ) ).sum(Array(0))
        val t1 = System.nanoTime()
        logger.info(s"Sum of MAP data chunk, size= %.2f M, Time-{ read: %.2f,  }, value = %.3f".format(bSize / 1.0E6, (t1 - t0) / 1.0E9, sum.getFlatValue(0)))
      case TestType.NcFile =>
        NetcdfDataset.setUseNaNs(false)
        val url = "file:" + outputNcFile
        try {
          logger.info( "Opening NetCDF dataset(3) at: " + url )
          val datset = NetcdfDataset.openDataset(url, true, bufferSize, null, null)
          Option(datset.findVariable(varName)) match {
            case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
            case Some(ncVar) =>
              runtime.printMemoryUsage(logger)
          }
        } catch {
          case e: java.io.IOException =>
            logger.error("Couldn't open dataset %s".format(url))
            throw e
          case ex: Exception =>
            logger.error("Something went wrong while reading %s".format(url))
            throw ex
        }
    }
  }

  def getShape( url: String, varName: String  ): Array[Int] = {
    try {
      val datset = NetcdfDataset.openDataset( url, true, -1, null, null)
      Option(datset.findVariable(varName)) match {
        case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
        case Some(ncVar) => ncVar.getShape
      }
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(url))
        throw e
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(url))
        throw ex
    }
  }
}

//class ncWriteTest extends Loggable {
//  import nasa.nccs.cdas.utilities.runtime
//  import java.nio.channels.FileChannel
//  import java.nio.file.StandardOpenOption._
//  val testType = TestType.Buffer
//
////  val url = "file:/Users/tpmaxwel/.cdas/cache/NCML/merra_daily.xml"
//  val url = "file:/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/NCML/merra_daily_2005.xml"
////  val outputFile = "/Users/tpmaxwel/.cdas/cache/test/testBinaryFile.out"
//  val outputFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testBinaryFile.out"
////  val outputNcFile = "/Users/tpmaxwel/.cdas/cache/test/testFile.nc"
//  val outputNcFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testFile.nc"
//  val bufferSize: Int = -1
//  val varName = "t"
//  NetcdfDataset.setUseNaNs(false)
////  new File(outputFile).delete()
////  new File(outputNcFile).delete()
//  try {
//    val datset = NetcdfDataset.openDataset(url, true, bufferSize, null, null )
//    Option(datset.findVariable(varName)) match {
//      case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
//      case Some( ncVar ) =>
//        runtime.printMemoryUsage(logger)
//        testType match  {
//        case TestType.Buffer =>
//          val t0 = System.nanoTime()
//          logger.info(s"Reading  $url...")
//          val data: ma2.Array = ncVar.read()
//          val t1 = System.nanoTime()
//          val bytes = data.getDataAsByteBuffer.array()
//          val outStr = new BufferedOutputStream(new FileOutputStream(new File(outputFile)))
//          logger.info(s"Writing Buffer $outputFile...")
//          runtime.printMemoryUsage(logger)
////          IOUtils.writeChunked(bytes, outStr)
//          IOUtils.write(bytes, outStr)
//          val t2 = System.nanoTime()
//          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ read: %.2f, write: %.2f, total: %.2f }".format(bytes.size / 1.0E6, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t2 - t0) / 1.0E9))
//        case TestType.Stream =>
//          val t0 = System.nanoTime()
//          logger.info(s"Reading  $url...")
//          val outStr = new BufferedOutputStream(new FileOutputStream(new File(outputFile)))
//          val size = ncVar.readToStream( ncVar.getShapeAsSection, outStr )
//          val t1 = System.nanoTime()
//          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ total: %.2f }".format( size / 1.0E6, (t1 - t0) / 1.0E9 ) )
//        case TestType.Channel =>
//          val t0 = System.nanoTime()
//          logger.info(s"Reading  $url...")
//          val channel = new RandomAccessFile(outputFile, "rw").getChannel()
//          val size = ncVar.readToByteChannel( ncVar.getShapeAsSection, channel )
//          val t1 = System.nanoTime()
//          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ total: %.2f }".format( size / 1.0E6, (t1 - t0) / 1.0E9 ) )
//        case TestType.Map =>
//          val t0 = System.nanoTime()
//          logger.info(s"Reading  $url...")
//          val bSize = ncVar.getSize * ncVar.getElementSize
//          var file = new RandomAccessFile(outputFile, "rw")
//          file.setLength( bSize )
//          val buffer =  file.getChannel.map( FileChannel.MapMode.READ_WRITE, 0, bSize );
//          val data = ncVar.read()
//          logger.info(s"Writing Map $outputFile")
//          runtime.printMemoryUsage(logger)
//          val t1 = System.nanoTime()
//          buffer.put( data.getDataAsByteBuffer )
//          buffer.force()
//          val t2 = System.nanoTime()
//          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ read: %.2f, write: %.2f, total: %.2f }".format( bSize / 1.0E6, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t2 - t0) / 1.0E9) )
//        case TestType.NcFile =>
//          val t0 = System.nanoTime()
//          logger.info(s"Reading  $url...")
//          val channel = new RandomAccessFile(outputFile, "rw").getChannel()
//          val bSize = ncVar.getSize * ncVar.getElementSize
//          val data = ncVar.read()
//          logger.info(s"Writing  $outputNcFile, size = %.2f M...".format( bSize / 1.0E6) )
//          runtime.printMemoryUsage(logger)
//          val t1 = System.nanoTime()
//          val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, outputNcFile )
//          datset.getDimensions.map( dim => writer.addDimension( null, dim.getShortName, dim.getLength, dim.isShared, dim.isUnlimited, dim.isVariableLength ) )
//          val newVar = writer.addVariable( null, ncVar.getShortName, ncVar.getDataType, ncVar.getDimensionsString )
//          writer.create()
//          writer.write( newVar, data )
//          writer.close()
//          val t2 = System.nanoTime()
//          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ read: %.2f, write: %.2f, total: %.2f }".format( bSize / 1.0E6, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t2 - t0) / 1.0E9) )
//      }
//    }
//  } catch {
//    case e: java.io.IOException =>
//      logger.error("Couldn't open dataset %s".format(url))
//      throw e
//    case ex: Exception =>
//      logger.error("Something went wrong while reading %s".format(url))
//      throw ex
//  }
//}


/*
object readTest extends App {
  val ncDataset: NetcdfDataset = NetcdfDataset.openDataset("/usr/local/web/WPS/CDAS2/src/test/resources/data/GISS-r1i1p1-sample.nc")
  val variable = ncDataset.findVariable(null, "tas")
  val section = new ma2.Section(Array(0, 0, 0), Array(1, 50, 50))
  val data = variable.read(section)
  print(data.getShape.mkString(","))
}

object writeTest extends App {
  val ncDataset: NetcdfDataset = NetcdfDataset.acquireDataset("/usr/local/web/WPS/CDAS2/src/test/resources/data/GISS-r1i1p1-sample.nc", null)
  val gridFilePath = "/tmp/gridFile.nc"
  println( "Creating Grid File at: " + gridFilePath )
  val gridWriter = NetcdfFileWriter.createNew( NetcdfFileWriter.Version.netcdf4, gridFilePath, null )
  val dimMap = Map( ncDataset.getDimensions.map( d => d.getShortName -> gridWriter.addDimension( null, d.getShortName, d.getLength ) ): _* )
  val varTups = for( cvar <- ncDataset.getVariables ) yield {
    val newVar = gridWriter.addVariable( null, cvar.getShortName, cvar.getDataType, cvar.getDimensionsString )
    println( "Add Varible: " + cvar.getShortName )
    cvar.getAttributes.map( attr => gridWriter.addVariableAttribute( newVar, attr ) )
    cvar.getShortName -> (cvar -> newVar)
  }
  val varMap = Map(varTups.toList:_*)
  val globalAttrs = Map( ncDataset.getGlobalAttributes.map( attr => attr.getShortName -> attr ): _*)
  globalAttrs.mapValues( attr => gridWriter.addGroupAttribute( null, attr ) )
  gridWriter.create()
  val boundsVars = for( ( cvar, newVar ) <- varMap.values; if cvar.isCoordinateVariable ) yield {
    println( " ** Write Variable: " + cvar.getShortName )
    gridWriter.write( newVar, cvar.read() )
    Option( cvar.findAttribute("bounds") )
  }
  boundsVars.flatten.map( bndsAttr => varMap.get(bndsAttr.getStringValue(0)) match {
    case Some( ( cvar, newVar ) ) =>
      println( " ** Write Bounds Variable: " + cvar.getShortName )
      gridWriter.write( newVar, cvar.read() )
    case None =>
      println( " ** Can't find Bounds Variable: " + bndsAttr.toString )
  })
  gridWriter.close()

}
*/


//object readTest extends App {
//  val  gridFilePath =  "/Users/tpmaxwel/.cdas/cache/collections/NCML/cip_cfsr_6hr_ta.nc"
//  val dset = NetcdfDataset.acquireDataset(gridFilePath, null)
//  val axis = dset.findCoordinateAxis( "time" )
//  axis.setCaching(true)
//  val axis1D = CoordinateAxis1DTime.factory( dset, axis, new Formatter() )
//  print( s"${axis1D.getSize} ${axis1D.getShape} \n" )
//  dset.close()
//}


// needs: DYLD_FALLBACK_LIBRARY_PATH=/Users/tpmaxwel/anaconda/envs/cdas2/lib
//object ncmlTest extends App {
//  val test_dir = new File("/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr")
//  val gridFile = "/Users/tpmaxwel/test.nc"
//  val ncmlFile = new File("/Users/tpmaxwel/test.xml")
//  val writer = NCMLWriter(test_dir)
//  writer.writeNCML(ncmlFile)
//  CDGrid.createGridFile(gridFile, ncmlFile.toString)
//  val dset = NetcdfDataset.acquireDataset(ncmlFile.toString, null)
//  println(dset.getVariables.toList.mkString(", "))
//
//  val origin = Array(1, 10, 10, 10)
//  val shape = Array(1, 1, 5, 5)
//  val section: ma2.Section = new ma2.Section(origin, shape)
//
//  //  val varName = "T"
////  dset.getVariables.toList.find(v => v.getShortName equals varName) match {
////    case Some(variable) =>
////      println("SHAPE: " + variable.getShape.mkString(", "))
////      val data = CDFloatArray.factory(variable.read(section), Float.NaN)
////      println(data.getArrayData().mkString(", "))
////    case None => println("Can't find variable " + varName + " in dataset " + ncmlFile.toString)
////  }
//}
//
//

//object gridFileTest extends App {
//  val gridFile = "/Users/tpmaxwel/.cdas/cache/collections/NCML/npana.nc"
//  val ncmlFile = "/Users/tpmaxwel/.cdas/cache/collections/NCML/npana.xml"
//  CDGrid.createGridFile(gridFile, ncmlFile)
//}
