package nasa.nccs.cdapi.cdm

import java.nio.channels.{FileChannel, NonReadableChannelException}

import nasa.nccs.esgf.process.DomainAxis
import nasa.nccs.cdapi.cdm
import ucar.nc2
import java.nio.file.{Paths, Files}
import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, Serializable, RandomAccessFile}
import java.nio._

import nasa.nccs.cds2.loaders.XmlResource
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateSystem, NetcdfDataset}

import scala.collection.mutable
import scala.collection.concurrent
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.xml.XML
// import scala.collection.JavaConverters._

object Collection {
  def apply( ctype: String, url: String, vars: List[String] = List() ) = {
    new Collection(ctype,url,vars)
  }
}
class Collection( val ctype: String, val url: String, val vars: List[String] = List() ) {
  def getUri( varName: String = "" ) = {
    ctype match {
      case "dods" => s"$url/$varName.ncml"
      case "file" => url
      case _ => throw new Exception( s"Unrecognized collection type: $ctype")
    }
  }
  override def toString = "Collection( type=%s, url=%s, vars=(%s))".format( ctype, url, vars.mkString(",") )
}

object DiskCacheFileMgr extends XmlResource {
  val diskCacheMap = loadDiskCacheMap

  def getDiskCacheFilePath( cachetype: String, cache_file: String ): String = {
    val cacheFilePath = Array( getDiskCache(), cachetype, cache_file ).mkString("/")
    Files.createDirectories( Paths.get(cacheFilePath).getParent )
    cacheFilePath
  }

  protected def getDiskCache( id: String = "main" ) = diskCacheMap.get(id) match {
    case None => throw new Exception( "No disk cache defined: " + id )
    case Some( diskCache ) =>
      diskCache.replaceFirst("^~",System.getProperty("user.home"))
  }

  protected def loadDiskCacheMap: Map[String,String] = {
    var filePath = getFilePath("/cache.xml")
    val tuples = XML.loadFile(filePath).child.map(
      node => node.attribute("id") match {
        case None => None;
        case Some(id) => node.attribute("path") match {
          case Some(path) => Some(id.toString -> path.toString)
          case None => None
        }
      })
    Map( tuples.flatten: _* )
  }
}

trait DiskCachable extends XmlResource {

  def getCacheType: String

  def sizeof[T]( value: T ) = value match {
    case _: Float => 4; case _: Short => 2; case _: Double => 8; case _: Int => 4; case _: Byte => 1
    case x => throw new Exception("Unsupported type in sizeof: " + x.toString)
  }

  protected def arrayToDisk[T <: AnyVal]( data: Array[T]  ): String = {
    val memsize = data.size * sizeof( data.head )
    val cache_file = "a" + System.nanoTime.toHexString
    try {
      val t0 = System.nanoTime()
      val channel = new RandomAccessFile(DiskCacheFileMgr.getDiskCacheFilePath(getCacheType, cache_file),"rw").getChannel()
      val buffer: MappedByteBuffer = channel.map( FileChannel.MapMode.READ_WRITE, 0, memsize )
      data.head match {
        case _: Float => buffer.asFloatBuffer.put(data.asInstanceOf[Array[Float]])
        case _: Short => buffer.asShortBuffer.put(data.asInstanceOf[Array[Short]])
        case _: Double => buffer.asDoubleBuffer.put(data.asInstanceOf[Array[Double]])
        case _: Int => buffer.asIntBuffer.put(data.asInstanceOf[Array[Int]])
        case _: Byte => buffer.put(data.asInstanceOf[Array[Byte]])
        case x => throw new Exception("Unsupported array type in arrayToDisk: " + x.toString)
      }
      channel.close
      val t1 = System.nanoTime()
      logger.info( s"Persisted cache data to file '%s', memsize = %d, time = %.2f".format( cache_file, memsize, (t1-t0)/1.0E9))
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

  protected def arrayFromDiskFloat( cache_id: String, size: Int  ): Option[Array[Float]] = {
    try {
      val t0 = System.nanoTime()
      getReadBuffer(cache_id) match { case ( channel, buffer ) =>
        val data: Array[Float] = Array.fill[Float](size)(0f)
        buffer.asFloatBuffer.get(data.asInstanceOf[Array[Float]])
        channel.close
        val t1 = System.nanoTime()
        logger.info( s"Restored persisted data to cache '%s', memsize = %d, time = %.2f".format( cache_id, size, (t1-t0)/1.0E9))
        Some(data)
      }
    } catch { case err: Throwable => logError(err,"Error retreiving persisted cache data"); None }
  }

  protected def arrayFromDiskByte( cache_id: String  ): Option[Array[Byte]] = {
    try { getReadBuffer(cache_id) match { case ( channel, buffer ) =>
        val data: Array[Byte] = Array.fill[Byte](channel.size.toInt)(0)
        buffer.get(data.asInstanceOf[Array[Byte]])
        channel.close
        Some(data)
      }
    } catch { case err: Throwable => logError(err,"Error retreiving persisted cache data"); None }
  }

}

object CDSDataset extends DiskCachable  {
  val cacheType = "dataset"
  def getCacheType: String = CDSDataset.cacheType


  def load( dsetName: String, collection: Collection, varName: String ): CDSDataset = {
    val uri = collection.getUri(varName)
    load(dsetName, uri, varName)
  }

  def restore( cache_rec_id: String ): CDSDataset = {
    val rec: CDSDatasetRec = objectFromDisk[CDSDatasetRec](cache_rec_id)
    load( rec.dsetName, rec.uri, rec.varName )
  }
  def persist( dset: CDSDataset ): String = objectToDisk( dset.getSerializable )

  def load( dsetName: String, uri: String, varName: String ): CDSDataset = {
    val t0 = System.nanoTime
    val ncDataset: NetcdfDataset = loadNetCDFDataSet( uri )
    val coordSystems: List[CoordinateSystem] = ncDataset.getCoordinateSystems.toList
    assert( coordSystems.size <= 1, "Multiple coordinate systems for one dataset is not supported" )
    if(coordSystems.isEmpty) throw new IllegalStateException("Error creating coordinate system for variable " + varName )
    val rv = new CDSDataset( dsetName, uri, ncDataset, varName, coordSystems.head )
    val t1 = System.nanoTime
    logger.info( "loadDataset(%s)T> %.4f,  ".format( uri, (t1-t0)/1.0E9 ) )
    rv
  }

  private def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    val dset_address = if ( url.toLowerCase().startsWith("file://") ) url.substring(6) else if ( url.toLowerCase().startsWith("file:") ) url.substring(5) else url
    try {
      logger.info("Opening NetCDF dataset %s".format(dset_address))
      NetcdfDataset.openDataset( dset_address )
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

class CDSDatasetRec( val dsetName: String, val uri: String, val varName: String ) extends Serializable {}

class CDSDataset( val name: String, val uri: String, val ncDataset: NetcdfDataset, val varName: String, val coordSystem: CoordinateSystem ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val attributes: List[nc2.Attribute] = ncDataset.getGlobalAttributes.map( a => { new nc2.Attribute( name + "--" + a.getFullName, a ) } ).toList
  val coordAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList

  def getCoordinateAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList

  def getSerializable = new CDSDatasetRec( name, uri, varName )

  def loadVariable( varName: String ): cdm.CDSVariable = {
    val t0 = System.nanoTime
    val ncVariable = ncDataset.findVariable(varName)
    if (ncVariable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
    val rv = new cdm.CDSVariable( varName, this, ncVariable )
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
