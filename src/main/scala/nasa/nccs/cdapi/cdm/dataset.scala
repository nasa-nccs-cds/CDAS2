package nasa.nccs.cdapi.cdm

import java.nio.channels.{FileChannel, NonReadableChannelException, ReadableByteChannel}

import nasa.nccs.caching.collectionDataCache
import ucar.{ma2, nc2}
import java.nio.file.{Files, Paths}
import java.io.{FileWriter, _}
import java.nio._

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.loaders.XmlResource
import nasa.nccs.utilities.Loggable
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateSystem, NetcdfDataset, VariableDS}
import ucar.nc2.ncml.NcMLReader
import ucar.nc2.util.DebugFlagsImpl

import scala.collection.mutable
import scala.collection.concurrent
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.xml.XML
import org.apache.commons.io.IOUtils

object Collection {
  def apply( id: String, url: String, path: String = "", fileFilter: String = "", scope: String="", vars: List[String] = List() ) = {
    new Collection(id,url,path,fileFilter,scope,vars)
  }
}
class Collection( val id: String="",  val url: String="", val path: String = "", val fileFilter: String = "", val scope: String="local", val vars: List[String] = List() ) extends Serializable with Loggable {
  val ctype: String = url.split(":").head
  val ncmlFile = new File( url.split(":").last )
  override def toString = "Collection( id=%s, url=%s, path=%s, fileFilter=%s )".format( id, url, path, fileFilter )
  def isEmpty = url.isEmpty

  def getUri( varName: String = "" ) = {
    ctype match {
      case "http" => s"$url/$varName.ncml"
      case _ => url
    }
  }

  def getDatasetMetadata(): List[nc2.Attribute] = {
    val dataset = collectionDataCache.getDataset( this, vars.head )
    val inner_attributes: List[nc2.Attribute] = List (
      new nc2.Attribute( "variables", vars.mkString(",") ),
      new nc2.Attribute( "path", path ),
      new nc2.Attribute( "url", url )
    )
    inner_attributes ++ dataset.attributes
  }

  def toXml: xml.Elem =
    if(path.isEmpty) {
      <collection id={id} url={url}>
        {vars.mkString(",")}
      </collection>
    } else if(fileFilter.isEmpty) {
      <collection id={id} url={url} path={path}>
        {vars.mkString(",")}
      </collection>
    } else {
      <collection id={id} url={url} path={path} fileFilter={fileFilter}>
        {vars.mkString(",")}
      </collection>
    }

  def createNCML( recreate: Boolean = false ): Boolean = {
    if( !ncmlFile.exists || recreate ) {
      ncmlFile.getParentFile.mkdirs
      val ncmlWriter = NCMLWriter(path)
      ncmlWriter.writeNCML(ncmlFile)
      true
    } else { false }
  }

}

object DiskCacheFileMgr extends XmlResource {
  val diskCacheMap = loadDiskCacheMap

  def getDiskCacheFilePath( cachetype: String, cache_file: String ): String =
    if (cache_file.startsWith("/")) {cache_file} else {
      val cacheFilePath = Paths.get( getCacheDirectory, cachetype, cache_file )
      Files.createDirectories( cacheFilePath.getParent )
      cacheFilePath.toString
    }

  def getCacheDirectory: String = {
    sys.env.get("CDAS_CACHE_DIR") match {
      case Some(cache_path) => cache_path
      case None =>
        val home = System.getProperty("user.home")
        Paths.get(home, ".cdas", "cache" ).toString
    }
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

  protected def bufferToDiskFloat( data: FloatBuffer  ): String = {
    val memsize = data.capacity() * 4
    val cache_file = "a" + System.nanoTime.toHexString
    try {
      val t0 = System.nanoTime()
      val cache_file_path = DiskCacheFileMgr.getDiskCacheFilePath(getCacheType, cache_file)
      val channel = new RandomAccessFile( cache_file_path, "rw" ).getChannel()
      val buffer: MappedByteBuffer = channel.map( FileChannel.MapMode.READ_WRITE, 0, memsize )
      buffer.asFloatBuffer.put(data)
      channel.close
      val t1 = System.nanoTime()
      logger.info( s"Persisted cache data to file '%s', memsize = %d, time = %.2f".format( cache_file_path, memsize, (t1-t0)/1.0E9))
      cache_file
    } catch {
      case err: Throwable => logError(err, s"Error writing data to disk, size = $memsize" ); ""
    }
  }

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

object CDSDataset extends DiskCachable  {
  val cacheType = "dataset"
  def getCacheType: String = CDSDataset.cacheType


  def load( collection: Collection, varName: String ): CDSDataset = {
    collection.createNCML()
    load(collection.url, collection, varName)
  }

  def restore( cache_rec_id: String ): CDSDataset = {
    val rec: CDSDatasetRec = objectFromDisk[CDSDatasetRec](cache_rec_id)
    load( rec.dsetName, rec.collection, rec.varName )
  }
  def persist( dset: CDSDataset ): String = objectToDisk( dset.getSerializable )

  def load( dsetName: String, collection: Collection, varName: String ): CDSDataset = {
    val t0 = System.nanoTime
    val uri = collection.getUri(varName)
    val ncDataset: NetcdfDataset = loadNetCDFDataSet( uri )
    val rv = new CDSDataset( dsetName, collection, ncDataset, varName, ncDataset.getCoordinateSystems.toList )
    val t1 = System.nanoTime
    logger.info( "loadDataset(%s)T> %.4f,  ".format( uri, (t1-t0)/1.0E9 ) )
    rv
  }

  def urlToPath(url: String): String = if ( url.toLowerCase().startsWith("file://") ) url.substring(6) else if ( url.toLowerCase().startsWith("file:") ) url.substring(5) else url

  private def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
//    NcMLReader.setDebugFlags( new DebugFlagsImpl( "NcML/debugURL NcML/debugXML NcML/showParsedXML NcML/debugCmd NcML/debugOpen NcML/debugConstruct NcML/debugAggDetail" ) )
    val dset_address = urlToPath(url)
    try {
      logger.info("Opening NetCDF dataset %s".format(dset_address))
      NetcdfDataset.openDataset( dset_address, true, null )
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(dset_address))
        throw e
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(dset_address))
        throw ex
    }
  }
}
//public class NcMLReader {
//  static private final Namespace ncNS = thredds.client.catalog.Catalog.ncmlNS;
//  static private org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NcMLReader.class);
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

class CDSDatasetRec( val dsetName: String, val collection: Collection, val varName: String ) extends Serializable {
  def getUri: String = collection.getUri(varName)
}

class CDSDataset( val name: String, val collection: Collection, val ncDataset: NetcdfDataset, val varName: String, coordSystems: List[CoordinateSystem] ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val attributes: List[nc2.Attribute] = ncDataset.getGlobalAttributes.map( a => { new nc2.Attribute( name + "--" + a.getFullName, a ) } ).toList
  val coordAxes: List[CoordinateAxis] = initCoordAxes
  val fileHeaders: Option[DatasetFileHeaders] = getDatasetFileHeaders

  def initCoordAxes(): List[CoordinateAxis] = {
    for( variable <- ncDataset.getVariables; if( variable.isCoordinateVariable ) ) {
      variable match {
        case cvar: VariableDS => ncDataset.addCoordinateAxis( variable.asInstanceOf[VariableDS] )
        case xvar => logger.warn( "Coordinate variable of improper type: " + xvar.getClass.getName )
      }
    }
    ncDataset.getCoordinateAxes.toList
  }

  def getCoordinateAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList
  def getFilePath = CDSDataset.urlToPath(collection.url)
  def getSerializable = new CDSDatasetRec( name, collection, varName )

  def getDatasetFileHeaders: Option[DatasetFileHeaders] = {
    val uri = collection.getUri(varName)
    if( uri.startsWith("http:" ) ) { None }
    else if( uri.endsWith(".xml" ) || uri.endsWith(".ncml" ) ) {
      val aggregation = XML.loadFile(getFilePath) \ "aggregation"
      val aggDim = (aggregation \ "@dimName").text
      val fileNodes = ( aggregation \ "netcdf" ).map( node => new FileHeader(  (node \ "@location").text,  (node \ "@coordValue").text.split(",").map( _.trim.toDouble ), false  ) )
      Some( new DatasetFileHeaders( aggDim, fileNodes ) )
    } else {
      None
    }
  }

  def loadVariable( varName: String ): CDSVariable = {
    val t0 = System.nanoTime
    val ncVariable = ncDataset.findVariable(varName)
    if (ncVariable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
    val rv = new CDSVariable( varName, this, ncVariable )
    val t1 = System.nanoTime
    logger.info( "loadVariable(%s)T> %.4f,  ".format( varName, (t1-t0)/1.0E9 ) )
    rv
  }

  def findCoordinateAxis( fullName: String ): Option[CoordinateAxis] = ncDataset.findCoordinateAxis( fullName ) match { case null => None; case x => Some( x ) }

//  def getCoordinateAxis( axisType: DomainAxis.Type.Value ): Option[CoordinateAxis] = {
//    axisType match {
//      case DomainAxis.Type.X => Option( coordSystem.getXaxis )
//      case DomainAxis.Type.Y => Option( coordSystem.getYaxis )
//      case DomainAxis.Type.Z => Option( coordSystem.getHeightAxis )
//      case DomainAxis.Type.Lon => Option( coordSystem.getLonAxis )
//      case DomainAxis.Type.Lat => Option( coordSystem.getLatAxis )
//      case DomainAxis.Type.Lev => Option( coordSystem.getPressureAxis )
//      case DomainAxis.Type.T => Option( coordSystem.getTaxis )
//    }
//  }
//
//  def getCoordinateAxis(axisType: Char): CoordinateAxis = {
//    axisType.toLower match {
//      case 'x' => if (coordSystem.isGeoXY) coordSystem.getXaxis else coordSystem.getLonAxis
//      case 'y' => if (coordSystem.isGeoXY) coordSystem.getYaxis else coordSystem.getLatAxis
//      case 'z' =>
//        if (coordSystem.containsAxisType(AxisType.Pressure)) coordSystem.getPressureAxis
//        else if (coordSystem.containsAxisType(AxisType.Height)) coordSystem.getHeightAxis else coordSystem.getZaxis
//      case 't' => coordSystem.getTaxis
//      case x => throw new Exception("Can't recognize axis type '%c'".format(x))
//    }
//  }
}

// var.findDimensionIndex(java.lang.String name)

object TestType {
  val Buffer = 0
  val Stream = 1
  val Channel = 2
  val Map = 3
  val NcFile = 4
}

object ncReadTest extends App with Loggable {

  import nasa.nccs.cds2.utilities.runtime
  import java.nio.channels.FileChannel
  import java.nio.file.StandardOpenOption._

  val testType = TestType.Map

  val url = "file:/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/NCML/merra_daily_2005.xml"
//  val outputFile = "/Users/tpmaxwel/.cdas/cache/test/testBinaryFile.out"
  val outputFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testBinaryFile.out"
//  val outputNcFile = "/Users/tpmaxwel/.cdas/cache/test/testFile.nc"
  val outputNcFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testFile.nc"
  val bufferSize: Int = -1
  val varName = "t"

  val shape = getShape( url, varName )
  testType match {
    case TestType.Buffer =>
      val t0 = System.nanoTime()
      logger.info(s"Reading  $outputFile...")
      val channel = FileChannel.open( new File(outputFile).toPath, READ )
      val bSize = channel.size.toInt
      val buffer = ByteBuffer.allocate(bSize)
      IOUtils.readFully(channel,buffer)
      val data = new CDFloatArray( shape, buffer.asFloatBuffer, Float.MaxValue )
      val sum = data.sum(Array(0))
      val t1 = System.nanoTime()
      logger.info( s"Sum of BUFFER data chunk, size= %.2f M, Time-{ read: %.2f,  }".format( bSize / 1.0E6, (t1 - t0) / 1.0E9 ) )
    case TestType.Map =>
      val t0 = System.nanoTime()
      logger.info(s"Reading  $outputFile...")
      val channel = FileChannel.open( new File(outputFile).toPath, READ )
      val bSize = channel.size.toInt
      val buffer = channel.map( FileChannel.MapMode.READ_ONLY, 0, bSize )
      val data = new CDFloatArray( shape, buffer.asFloatBuffer, Float.MaxValue )
      val sum = data.sum(Array(0))
      val t1 = System.nanoTime()
      logger.info( s"Sum of MAP data chunk, size= %.2f M, Time-{ read: %.2f,  }".format( bSize / 1.0E6, (t1 - t0) / 1.0E9 ) )
    case TestType.NcFile =>
      NetcdfDataset.setUseNaNs(false)
      val url = "file:"+outputNcFile
      try {
        val datset = NetcdfDataset.openDataset( url, true, bufferSize, null, null)
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

object ncWriteTest extends App with Loggable {
  import nasa.nccs.cds2.utilities.runtime
  import java.nio.channels.FileChannel
  import java.nio.file.StandardOpenOption._
  val testType = TestType.Buffer

//  val url = "file:/Users/tpmaxwel/.cdas/cache/NCML/merra_daily.xml"
  val url = "file:/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/NCML/merra_daily_2005.xml"
//  val outputFile = "/Users/tpmaxwel/.cdas/cache/test/testBinaryFile.out"
  val outputFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testBinaryFile.out"
//  val outputNcFile = "/Users/tpmaxwel/.cdas/cache/test/testFile.nc"
  val outputNcFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/test/testFile.nc"
  val bufferSize: Int = -1
  val varName = "t"
  NetcdfDataset.setUseNaNs(false)
//  new File(outputFile).delete()
//  new File(outputNcFile).delete()
  try {
    val datset = NetcdfDataset.openDataset(url, true, bufferSize, null, null )
    Option(datset.findVariable(varName)) match {
      case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
      case Some( ncVar ) =>
        runtime.printMemoryUsage(logger)
        testType match  {
        case TestType.Buffer =>
          val t0 = System.nanoTime()
          logger.info(s"Reading  $url...")
          val data: ma2.Array = ncVar.read()
          val t1 = System.nanoTime()
          val bytes = data.getDataAsByteBuffer.array()
          val outStr = new BufferedOutputStream(new FileOutputStream(new File(outputFile)))
          logger.info(s"Writing Buffer $outputFile...")
          runtime.printMemoryUsage(logger)
//          IOUtils.writeChunked(bytes, outStr)
          IOUtils.write(bytes, outStr)
          val t2 = System.nanoTime()
          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ read: %.2f, write: %.2f, total: %.2f }".format(bytes.size / 1.0E6, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t2 - t0) / 1.0E9))
        case TestType.Stream =>
          val t0 = System.nanoTime()
          logger.info(s"Reading  $url...")
          val outStr = new BufferedOutputStream(new FileOutputStream(new File(outputFile)))
          val size = ncVar.readToStream( ncVar.getShapeAsSection, outStr )
          val t1 = System.nanoTime()
          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ total: %.2f }".format( size / 1.0E6, (t1 - t0) / 1.0E9 ) )
        case TestType.Channel =>
          val t0 = System.nanoTime()
          logger.info(s"Reading  $url...")
          val channel = new RandomAccessFile(outputFile, "rw").getChannel()
          val size = ncVar.readToByteChannel( ncVar.getShapeAsSection, channel )
          val t1 = System.nanoTime()
          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ total: %.2f }".format( size / 1.0E6, (t1 - t0) / 1.0E9 ) )
        case TestType.Map =>
          val t0 = System.nanoTime()
          logger.info(s"Reading  $url...")
          val bSize = ncVar.getSize * ncVar.getElementSize
          var file = new RandomAccessFile(outputFile, "rw")
          file.setLength( bSize )
          val buffer =  file.getChannel.map( FileChannel.MapMode.READ_WRITE, 0, bSize );
          val data = ncVar.read()
          logger.info(s"Writing Map $outputFile")
          runtime.printMemoryUsage(logger)
          val t1 = System.nanoTime()
          buffer.put( data.getDataAsByteBuffer )
          buffer.force()
          val t2 = System.nanoTime()
          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ read: %.2f, write: %.2f, total: %.2f }".format( bSize / 1.0E6, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t2 - t0) / 1.0E9) )
        case TestType.NcFile =>
          val t0 = System.nanoTime()
          logger.info(s"Reading  $url...")
          val channel = new RandomAccessFile(outputFile, "rw").getChannel()
          val bSize = ncVar.getSize * ncVar.getElementSize
          val data = ncVar.read()
          logger.info(s"Writing  $outputNcFile, size = %.2f M...".format( bSize / 1.0E6) )
          runtime.printMemoryUsage(logger)
          val t1 = System.nanoTime()
          val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, outputNcFile )
          datset.getDimensions.map( dim => writer.addDimension( null, dim.getShortName, dim.getLength, dim.isShared, dim.isUnlimited, dim.isVariableLength ) )
          val newVar = writer.addVariable( null, ncVar.getShortName, ncVar.getDataType, ncVar.getDimensionsString )
          writer.create()
          writer.write( newVar, data )
          writer.close()
          val t2 = System.nanoTime()
          logger.info(s"Persisted data chunk, size= %.2f M, Times-{ read: %.2f, write: %.2f, total: %.2f }".format( bSize / 1.0E6, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t2 - t0) / 1.0E9) )
      }
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