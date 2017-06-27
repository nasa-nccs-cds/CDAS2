package nasa.nccs.cdapi.cdm

import java.nio.channels.{FileChannel, NonReadableChannelException, ReadableByteChannel}

import ucar.{ma2, nc2}
import java.nio.file.{Files, Path, Paths}
import java.io.{FileWriter, _}
import java.net.URI
import java.nio._
import java.util.Formatter

import scala.xml
import scala.concurrent.ExecutionContext.Implicits.global
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.caching.{CDASCachePartitioner, CDASPartitioner}
import nasa.nccs.cdapi.data.HeapFltArray
import nasa.nccs.cdapi.tensors.{CDDoubleArray, CDFloatArray, CDLongArray}
import nasa.nccs.cdas.loaders.XmlResource
import nasa.nccs.cdas.utilities.{appParameters, runtime}
import nasa.nccs.utilities.{Loggable, cdsutils}
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset._
import ucar.ma2
import ucar.nc2.constants.CDM

import scala.collection.{concurrent, mutable}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.xml.XML
import nasa.nccs.cdas.loaders.Collections
import nasa.nccs.cdas.workers.TransVar
import nasa.nccs.cdas.workers.python.{PythonWorker, PythonWorkerPortal}
import nasa.nccs.esgf.process.{CDSection, DataSource}
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import ucar.nc2._
import ucar.nc2.write.Nc4Chunking

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
    val gridDS = NetcdfDatasetMgr.open(gridFilePath)
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

  def testNc4(): Unit = {
    val iospClass = this.getClass.getClassLoader.loadClass("ucar.nc2.jni.netcdf.Nc4Iosp")
    val ctor = iospClass.getConstructor(classOf[NetcdfFileWriter.Version])
    val spi = ctor.newInstance(NetcdfFileWriter.Version.netcdf4)
    val method = iospClass.getMethod("setChunker", classOf[Nc4Chunking])
    method.invoke( spi, null )
  }

  def getNumFiles( ncDataset: NetcdfDataset ): Int = try {
    ncDataset.getAggregation.getDatasets.size()
  } catch { case ex: Exception => 1 }

  def createGridFile(gridFilePath: String, datfilePath: String) = {
    logger.info( s"Creating #grid# file $gridFilePath from datfilePath: $datfilePath" )
    testNc4()
    val ncDataset: NetcdfDataset = NetcdfDatasetMgr.open(datfilePath)
    val gridWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, gridFilePath, null)
    val dimMap = Map(ncDataset.getDimensions.map(d => NCMLWriter.getName(d) -> gridWriter.addDimension(null, NCMLWriter.getName(d), d.getLength)): _*)
    val groupMap = mutable.HashMap.empty[String,nc2.Group]
    var nDataFiles = getNumFiles( ncDataset )

    val varTups = for (cvar <- ncDataset.getVariables) yield {
      val dataType = cvar match {
        case coordAxis: CoordinateAxis =>
          if(coordAxis.getAxisType == AxisType.Time) ma2.DataType.LONG
          else cvar.getDataType
        case x => cvar.getDataType
      }
      val oldGroup = cvar.getGroup
      val newGroup = getNewGroup( groupMap, oldGroup, gridWriter )
      val newVar: nc2.Variable = gridWriter.addVariable( newGroup, NCMLWriter.getName(cvar), dataType, getDimensionNames( cvar.getDimensionsString.split(' '), dimMap.keys ).mkString(" ")  )
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
    gridWriter.addGroupAttribute( null, new Attribute("NumDataFiles",nDataFiles) )
    gridWriter.create()
    for ((cvar, newVar) <- varMap.values; if cvar.isCoordinateVariable && (cvar.getRank == 1) ) cvar match  {
      case coordAxis: CoordinateAxis =>
        val boundsVarOpt = Option(coordAxis.getBoundaryRef) match {
          case Some(bref) => Some(bref)
          case None => Option(coordAxis.findAttributeIgnoreCase("bounds")) match {
            case Some(attr) => Some(attr.getStringValue)
            case None =>
              logger.warn("Can't locate bounds for axis " + coordAxis.getShortName + " in file " + datfilePath + ", vars = " + ncDataset.getVariables.map(_.getShortName).mkString(",") )
              None
          }
        }
        if( coordAxis.getAxisType == AxisType.Time ) {
          val ( time_values, bounds ) = FileHeader.getTimeValues( ncDataset, coordAxis )
          newVar.addAttribute( new Attribute( CDM.UNITS, cdsutils.baseTimeUnits ) )
          gridWriter.write( newVar, ma2.Array.factory( ma2.DataType.LONG, coordAxis.getShape, time_values ) )
          boundsVarOpt flatMap varMap.get match {
            case Some( ( cvarBnds, newVarBnds )  ) => gridWriter.write( newVarBnds, ma2.Array.factory( ma2.DataType.DOUBLE, cvarBnds.getShape, bounds ) )
            case None => Unit
          }
        } else {
          gridWriter.write(newVar, coordAxis.read())
          coordAxis match {
            case coordAxis1D: CoordinateAxis1D =>
              boundsVarOpt flatMap varMap.get match {
                case Some((cvarBnds, newVarBnds)) =>
                  val bounds: Array[Double] = ((0 until coordAxis1D.getShape(0)) map (index => coordAxis1D.getCoordBounds(index))).toArray.flatten
                  gridWriter.write(newVarBnds, ma2.Array.factory(ma2.DataType.DOUBLE, cvarBnds.getShape, bounds))
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
    NetcdfDatasetMgr.close(datfilePath)
  }
}

class CDGrid( val name: String,  val gridFilePath: String, val coordAxes: List[CoordinateAxis], val coordSystems: List[CoordinateSystem], val dimensions: List[Dimension], val attributes: List[nc2.Attribute] ) extends Loggable {
  val precache = false

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
    val gridDS = NetcdfDatasetMgr.open(gridFilePath)
    try {
      val axisOpt = Option( gridDS.findCoordinateAxis( name ) )
      axisOpt.map( axis => {
          if (precache) { axis.setCaching(true); axis.read() }
          axis
        }
      )
    } catch {
      case err: Exception =>
        logger.error("Can't find Coordinate Axis " + name + " in gridFile " + gridFilePath + " , error = " + err.toString );
        logger.error(err.getStackTrace.mkString("\n"))
        None
    }
  }

  def getTimeCoordinateAxis: Option[CoordinateAxis1DTime] = {
    val gridDS = NetcdfDatasetMgr.open(gridFilePath)
    try {
      val axisOpt = Option( gridDS.findCoordinateAxis( AxisType.Time ) )
      axisOpt.map( axis => {
        if (precache) { axis.setCaching(true); axis.read() }
        CoordinateAxis1DTime.factory( gridDS, axis, new Formatter() )
      })
    } catch {
      case err: Exception =>
        logger.error("Can't create time Coordinate Axis for collection " + name + " in gridFile " + gridFilePath + ", error = " + err.toString );
        logger.error(err.getStackTrace.mkString("\n"))
        None
    }
  }


  def findCoordinateAxis( atype: AxisType ): Option[CoordinateAxis] = {
    val gridDS = NetcdfDatasetMgr.open(gridFilePath)
    try {
      Option( gridDS.findCoordinateAxis( atype ) ).map( axis => {
        if (precache) { axis.setCaching(true); axis.read() }
        axis
      } )
    } catch {
      case err: Exception =>
        logger.error("Can't find Coordinate Axis with type: " + atype.toString + " in gridFile " + gridFilePath + ", error = " + err.toString  );
        logger.error(err.getStackTrace.mkString("\n"))
        None
    }
  }

  def getVariable( varShortName: String ): ( Int, nc2.Variable ) = {
    val ncDataset: NetcdfDataset = NetcdfDatasetMgr.open( gridFilePath )
    val numDataFiles: Int = ncDataset.findGlobalAttribute("NumDataFiles").getNumericValue.intValue()
    val variables = ncDataset.getVariables.toList
    variables.find ( v => (v.getShortName equals varShortName) ) match {
      case Some( variable ) => ( numDataFiles, variable )
      case None => throw new Exception("Can't find variable %s in collection %s (%s), variable names = [ %s ] ".format(varShortName,name,gridFilePath, variables.map(_.getShortName).mkString(", ") ) )
    }
  }

  def getAttribute( keyValuePair: (String, Option[String] )  ): Option[nc2.Attribute] = keyValuePair._2 match {
    case Some( value ) => if(value.isEmpty) None else Some( new nc2.Attribute( keyValuePair._1, value ) )
    case None => None
  }

  def getVariableMetadata( varShortName: String ): List[nc2.Attribute] = {
    val ( numDataFiles, ncVariable ) = getVariable( varShortName )
    val attributes = ncVariable.getAttributes.toList
    val keyValuePairs = List(
      "description" -> ncVariable.getDescription,
      "units" -> ncVariable.getUnitsString,
      "dtype" -> ncVariable.getDataType.toString,
      "dims" -> ncVariable.getDimensionsString,
      "shape" -> ncVariable.getShape.mkString(","),
      "fullname" -> ncVariable.getFullName,
      "numDataFiles" -> numDataFiles.toString
    ) map { case (key,value) => getAttribute(key, Option(value)) }
    attributes ++ keyValuePairs.flatten
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
    val ncDataset: NetcdfDataset = NetcdfDatasetMgr.open(grid.gridFilePath)
    try {
      _aggCollection(ncDataset)
    } catch {
      case err: Exception => logger.error("Can't aggregate collection for dataset " + ncDataset.toString); throw err
    }
  }

  def readVariableData(varShortName: String, section: ma2.Section): ma2.Array =
    NetcdfDatasetMgr.readVariableData(varShortName, dataPath, section )

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

class bigDataTest extends Loggable {
  val serverConfiguration = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )
  val service = "cds2"

//  def main(args: Array[String]): Unit = {
//    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"file://att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/npana.xml","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"tz"}]]"""
//    val result_node = executeTest(datainputs)
////    val result_data = getResultData( result_node )
//  }

  def getDataNodes( result_node: xml.Elem, print_result: Boolean = false  ): xml.NodeSeq = {
    if(print_result) { println( s"Result Node:\n${result_node.toString}\n" ) }
    result_node.label match {
      case "response" => result_node \\ "outputs" \\ "data"
      case _ => result_node \\ "Output" \\ "LiteralData"
    }
  }

  def getResultData( result_node: xml.Elem, print_result: Boolean = false ): CDFloatArray = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, print_result )
    try{  CDFloatArray( data_nodes.head.text.split(',').map(_.toFloat), Float.MaxValue ) } catch { case err: Exception => CDFloatArray.empty }
  }

  def getResultValue( result_node: xml.Elem ): Float = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node )
    try{ data_nodes.head.text.toFloat } catch { case err: Exception => Float.NaN }
  }

  def executeTest( datainputs: String, async: Boolean = false, identifier: String = "CDSpark.workflow" ): xml.Elem = {
    val t0 = System.nanoTime()
    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString, "unitTest" -> "true" )
    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
    val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
    for( child_node <- response.child ) if ( child_node.label.startsWith("exception")) { throw new Exception( child_node.toString ) }
    println("Completed test '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    response
  }

}



class profilingTest extends Loggable {

  def computeMax1(data: ma2.Array): Float = {
    var max = Float.MinValue
    while (data.hasNext()) {
      val dval = data.nextFloat();
      if (!dval.isNaN) {
        max = Math.max(max, dval)
      }
    }
    if (max == Float.MinValue) Float.NaN else max
  }

  def computeMax(data: ma2.Array): Float = CDFloatArray.factory(data,Float.NaN).max().getStorageData.get(0)

  def computeMax3(data: ma2.Array): Float = {
    val fltArray = CDFloatArray.factory(data,Float.NaN)
    var max = Float.MinValue
    for ( index <-( 0 until fltArray.getSize ) ) {
      val dval = fltArray.getStorageValue( index )
      if (!dval.isNaN) { max = Math.max(max, dval) }
    }
    if (max == Float.MinValue) Float.NaN else max
  }

  def computeMax2( data: CDFloatArray ): Float = {
    var max = Float.MinValue
    val datasize = data.getSize
    for( index <- 0 until datasize; dval = data.getFlatValue(index); if !dval.isNaN ) { max = Math.max(max, dval) }
    if (max == Float.MinValue) Float.NaN else max
  }

//  def processCacheData(cache_id: String, roi: ma2.Section) = {
//    val partitioner = new CDASCachePartitioner(cache_id, roi)
//    val t0 = System.nanoTime()
//    val full_shape = partitioner.getShape
//    var total_read_time = 0.0
//    var total_compute_time = 0.0
//    println("Processing data, full shape = " + full_shape.mkString(", "))
//    val partitions = partitioner.getCachePartitions
//    for (partition <- partitions) {
//      val itime = partition.startIndex
//      val chunk_size = partition.shape(0)
//      val ncycle = chunk_size * (partition.index + 1)
//      val chunk_origin = partition.origin
//      val chunk_shape = partition.shape
//      val ts0 = System.nanoTime()
//      val cfdata: CDFloatArray = partition.data(Float.NaN)
//      println("Mapped data, P[%d]: data shape = (%s), datasize = %d, ncycles = %d, chunk_size = %d".format( partition.index, cfdata.getShape.mkString(", "), cfdata.getSize, cfdata.getSize/(full_shape(2)*full_shape(3)), chunk_size ) )
//      val ts1 = System.nanoTime()
//      val max = computeMax(cfdata)
//      val ts2 = System.nanoTime()
//      val read_time = (ts1 - ts0) / 1.0E9
//      val compute_time = (ts2 - ts1) / 1.0E9
//      total_read_time += read_time
//      total_compute_time += compute_time
//      println("Computed max = %.4f [time=%d] in %.4f sec, data read time = %.4f sec, compute time = %.4f sec".format(max, itime, read_time + compute_time, read_time, compute_time) )
//      println("Aggretate time for %d cycles = %.4f sec".format(ncycle, (ts2 - t0) / 1.0E9))
//      println("Average over %d cycles: read time per cycle = %.4f sec, compute time per cycle = %.4f sec".format(ncycle, total_read_time / ncycle, total_compute_time / ncycle))
//    }
//    println("Completed data processing for collection '%s' in %.4f sec".format(partitioner.cache_id, (System.nanoTime() - t0) / 1.0E9))
//  }

  def processFileData(ncmlFile: String, gridFile: String, varName: String) = {
    try {
      val datset = NetcdfDataset.openDataset(ncmlFile, true, -1, null, null)
      Option(datset.findVariable(varName)) match {
        case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
        case Some(ncVar) => processDataPython(ncVar,gridFile)
      }
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(ncmlFile))
        throw e
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(ncmlFile))
        throw ex
    }
  }

  def processDataPython(variable: Variable, gridFile: String) = {
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance();
    val worker: PythonWorker = workerManager.getPythonWorker();
    val t0 = System.nanoTime()
    val full_shape = variable.getShape
    val test_section = Array( 10, 10 )
    val test_origin = Array( 140, 140 )
    var total_read_time = 0.0
    var total_compute_time = 0.0
    val chunk_size = 1
    val attrs = variable.getAttributes.iterator().map( _.getShortName ).mkString(", ")
    val mem_size = (chunk_size*4*full_shape(2)*full_shape(3))/1.0E6
    val missing = variable.findAttributeIgnoreCase("fmissing_value").getNumericValue.floatValue()
    val isNaN = missing.isNaN
    println("Processing data, full shape = %s, attrs = %s".format( full_shape.mkString(", "), attrs ))
    println(s"Missing value = %.4f, isNaN = %s".format( missing, isNaN.toString ) )

    (0 until full_shape(0) by chunk_size) foreach (itime => {
      val ncycle = (full_shape(1) * (itime + 1))
//      val chunk_origin = Array[Int](itime, ilevel, test_origin(0), test_origin(1) )
//      val chunk_shape = Array[Int]( chunk_size, 1, test_section(0), test_section(1) )
      val chunk_origin = Array[Int](itime, 0, 0, 0 )
      val chunk_shape = Array[Int]( chunk_size, full_shape(1), full_shape(2), full_shape(3) )
      val ts0 = System.nanoTime()
      val data = variable.read(chunk_origin, chunk_shape)
      val ts1 = System.nanoTime()
      val rID = "r" + ncycle.toString

      val metadata: Map[String, String] = Map( "name" -> variable.getShortName, "collection" -> "npana", "gridfile" -> gridFile, "dimensions" -> variable.getDimensionsString,
        "units" -> variable.getUnitsString, "longname" -> variable.getFullName, "uid" -> variable.getShortName, "roi" -> CDSection.serialize(new ma2.Section(chunk_origin,chunk_shape)) )
      val op_metadata: Map[String, String] = Map.empty[String,String] // Map( "axis" -> "x" ) // Map.empty[String,String]
      worker.sendRequestInput( variable.getShortName, HeapFltArray( data, chunk_origin, gridFile, metadata, missing ) )
      worker.sendRequest("python.numpyModule.max-"+rID, Array(variable.getShortName), op_metadata )
      val tvar: TransVar = worker.getResult()
      val result = HeapFltArray( tvar, Some(gridFile) )
      val ts2 = System.nanoTime()
      val read_time = (ts1 - ts0) / 1.0E9
      val compute_time = (ts2 - ts1) / 1.0E9
      total_read_time += read_time
      total_compute_time += compute_time
      println("Computed max = %.4f [time=%d, nts=%d] in %.4f sec per ts, data read time per ts = %.4f sec, compute time per ts = %.4f sec".format( result.data(0), itime, chunk_size, (read_time + compute_time)/chunk_size, read_time/chunk_size, compute_time/chunk_size))
      println("Aggretate time for %d cycles = %.4f sec, chunk mem size = %.2f MB".format( ncycle, (ts2 - t0) / 1.0E9, mem_size ))
      println("Average over %d cycles: read time per tstep = %.4f sec, compute time per tstep = %.4f sec".format(ncycle, total_read_time / ncycle, total_compute_time / ncycle ))
    })
    println("Completed data processing for '%s' in %.4f sec".format(variable.getFullName, (System.nanoTime() - t0) / 1.0E9))
  }


  def processData(variable: Variable) = {
    val t0 = System.nanoTime()
    val full_shape = variable.getShape
    var total_read_time = 0.0
    var total_compute_time = 0.0
    val chunk_size = 1
    println("Processing data, full shape = " + full_shape.mkString(", "))
    (0 until full_shape(1)) foreach (ilevel => {
      (0 until full_shape(0) by chunk_size) foreach (itime => {
        val ncycle = ilevel * full_shape(0) + itime + 1
        val chunk_origin = Array[Int](itime, ilevel, 0, 0)
        val chunk_shape = Array[Int](chunk_size, 1, full_shape(2), full_shape(3))
        val ts0 = System.nanoTime()
        val data = variable.read(chunk_origin, chunk_shape)
        val ts1 = System.nanoTime()
        val max = computeMax(data)
        val ts2 = System.nanoTime()
        val read_time = (ts1 - ts0) / 1.0E9
        val compute_time = (ts2 - ts1) / 1.0E9
        total_read_time += read_time
        total_compute_time += compute_time
        println("Computed max = %.4f [time=%d, level=%d] in %.4f sec, data read time = %.4f sec, compute time = %.4f sec".format(max, itime, ilevel, read_time + compute_time, read_time, compute_time))
        println("Aggretate time for %d cycles = %.4f sec".format(ncycle, (ts2 - t0) / 1.0E9))
        println("Average over %d cycles: read time per cycle = %.4f sec, compute time per cycle = %.4f sec".format(ncycle, total_read_time / ncycle, total_compute_time / ncycle))
      })
    })
    println("Completed data processing for '%s' in %.4f sec".format(variable.getFullName, (System.nanoTime() - t0) / 1.0E9))
  }

//  def main(args: Array[String]): Unit = {
//    val ncmlFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/npana.xml"
//    val gridFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/npana.nc"
//    val cache_id = "a3298cb50c2abb"
//    val varName = "T"
//    val iLevel = 10
//    val roi_origin = Array[Int](0, iLevel, 0, 0)
//    val roi_shape = Array[Int](53668, 1, 361, 576)
//    val roi = new ma2.Section(roi_origin, roi_shape)
//    processFileData( ncmlFile, gridFile, varName )
//  }
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

  def readBuffer(input: InputStream, buffer: Array[Byte]): Int = readBuffer(input, buffer, 0, buffer.length)

  def readBuffer(input: InputStream, buffer: Array[Byte], offset: Int, length: Int): Int = {
    if (length < 0) throw new IllegalArgumentException("Length must not be negative: " + length)
    var remaining = length
    while (remaining > 0) {
      val location = length - remaining
      val count = input.read(buffer, offset + location, remaining)
      if (-1 == count) { remaining = -1 }
      else { remaining -= count }
    }
    length - remaining
  }

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
        readBuffer(inputStream, buffer)
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

object NetcdfDatasetMgr extends Loggable {
//  NetcdfDataset.initNetcdfFileCache(10,1000,3600)   // Bugs in Netcdf file caching cause NullPointerExceptions on MERRA2 npana datasets (var T): ( 3/3/2017 )
  val datasetCache = new ConcurrentLinkedHashMap.Builder[String, NetcdfDataset].initialCapacity(64).maximumWeightedCapacity(1000).build()
  val MB = 1024*1024

  def readVariableData(varShortName: String, dataPath: String, section: ma2.Section): ma2.Array = {
    val ncDataset: NetcdfDataset = open( dataPath )
    val result = ncDataset.getVariables.toList.find( v => v.getShortName equals varShortName ) match {
      case Some(variable) =>
        try {
          runtime.printMemoryUsage(logger)
          val t0 = System.nanoTime()
          val ma2array = variable.read(section)
          val sample_data = ( 0 until Math.min(16,ma2array.getSize).toInt ) map ma2array.getFloat
          logger.info( "Reading variable %s, section shape: (%s), section origin: (%s), variable shape: (%s), size = %.2f M, read time = %.4f sec, sample data = [ %s ]".format( varShortName, section.getShape.mkString(","), section.getOrigin.mkString(","), variable.getShape.mkString(","), (section.computeSize*4.0)/MB, (System.nanoTime() - t0) / 1.0E9, sample_data.mkString(", ") ))
          ma2array
        } catch {
          case err: Exception =>
            logger.error("Can't read data for variable %s in dataset %s due to error: %s".format(varShortName, ncDataset.getLocation, err.toString));
            logger.error("Variable shape: (%s),  section: { o:(%s) s:(%s) }".format(variable.getShape.mkString(","), section.getOrigin.mkString(","), section.getShape.mkString(",")));
            logger.error(err.getStackTrace.map(_.toString).mkString("\n"))
            throw err
        }
      case None => throw new Exception( s"Can't find variable $varShortName in dataset $dataPath ")
    }
    close( dataPath )
    result
  }

  def keys: Set[String] = datasetCache.keySet().toSet
  def values: Iterable[NetcdfDataset] = datasetCache.values()
  def getKey( path: String ): String =  path + ":" + Thread.currentThread().getId()

  def open(path: String ): NetcdfDataset = {
    val cpath = cleanPath(path)
    val key = getKey(path)
    val result = datasetCache.getOrElseUpdate( key, acquireDataset(cpath) )
//    logger.info(s"   Accessed Dataset using key: $key, path: $cpath")
    result
  }

  def cleanPath( path: String ): String =
    if( path.startsWith("file://") ) path.substring(6)
    else if( path.startsWith("file:/") ) path.substring(5)
    else path

  def closeAll: Iterable[NetcdfDataset]  = keys flatMap _close
  private def _close( key: String ): Option[NetcdfDataset] = Option( datasetCache.remove( key ) ).map ( dataset => { dataset.close(); dataset } )
  def close( path: String ): Option[NetcdfDataset] = _close( getKey(cleanPath(path)) )

  private def acquireDataset( path: String ): NetcdfDataset = {
//    val result = NetcdfDataset.acquireDataset(path, null)
    val result = NetcdfDataset.openDataset(path)
    logger.info(s"   Opened Dataset from path: $path   ")
    result
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
//  val gridFilePath = "/Users/tpmaxwel/.cdas/cache/collections/NCML/npana.nc"
//  val gridDS = NetcdfDatasetMgr.open( gridFilePath )
//  val name = "time"
//  try {
//    val axisOpt = Option( gridDS.findCoordinateAxis( name ) )
//    axisOpt.map( axis => {
//      axis.setCaching(true);
//      val raw_data = axis.read()
//      val dataArray = CDLongArray.factory(raw_data)
//      print( dataArray.getArrayData().mkString(", ") )
//      NetcdfDatasetMgr.close( gridFilePath )
//    } )
//  } catch {
//    case err: Exception =>
//      print("Can't find Coordinate Axis " + name + " in gridFile " + gridFilePath + " , error = " + err.toString );
//      None
//  }
//}

//object dataFileTest extends App {
//  val gridFilePath = "/Users/tpmaxwel/.cdas/cache/collections/NCML/npana.nc"
//  val gridDS = NetcdfDatasetMgr.open( gridFilePath )
//  val dataPath = "/Users/tpmaxwel/.cdas/cache/collections/NCML/npana.xml"
//  val ncDataset = NetcdfDatasetMgr.open(dataPath)
//  val varShortName = "T"
//  val origin = Array(0,40,0,0)
//  val shape = Array(12,1,361,576)
//  val section: ma2.Section = new ma2.Section(origin, shape)
//  ncDataset.getVariables.toList.find(v => v.getShortName equals varShortName) match {
//    case Some(variable) =>
//      try {
//        val raw_data = variable.read(section)
//        val dataArray = CDFloatArray.factory( raw_data, Float.NaN )
//        println( dataArray.getArrayData(25).mkString(", ") )
//        NetcdfDatasetMgr.close( dataPath )
//      } catch {
//        case err: Exception =>
//          println("Can't read data for variable %s in dataset %s due to error: %s".format(varShortName, ncDataset.getLocation, err.toString));
//          println("Variable shape: (%s),  section: { o:(%s) s:(%s) }".format(variable.getShape.mkString(","), section.getOrigin.mkString(","), section.getShape.mkString(",")));
//          println(err.getStackTrace.map(_.toString).mkString("\n"))
//      }
//    case None => println(s"Can't find variable $varShortName in dataset $dataPath ")
//  }
//}
//
//


//object treadIdTest extends App {
//  println( Thread.currentThread().getId() )
//  val yf0 = Future[Long] { Thread.sleep(3000); Thread.currentThread().getId() }
//  val yf1 = Future[Long] { Thread.sleep(3000); Thread.currentThread().getId() }
//  val yf2 = Future[Long] { Thread.sleep(3000); Thread.currentThread().getId() }
//  println( Await.result( yf0, Duration.Inf) )
//  println( Await.result( yf1, Duration.Inf) )
//  println( Await.result( yf2, Duration.Inf) )
//}
