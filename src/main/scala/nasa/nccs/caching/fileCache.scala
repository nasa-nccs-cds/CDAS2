package nasa.nccs.caching

import java.io.{FileInputStream, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, FloatBuffer, MappedByteBuffer}

import nasa.nccs.cdapi.cdm.{CDSVariable, DiskCacheFileMgr}
import nasa.nccs.cdapi.tensors.{CDByteArray, CDFloatArray}
import nasa.nccs.esgf.process.DataFragmentSpec
import nasa.nccs.utilities.Loggable
import ucar.ma2

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CacheChunk( val offset: Int, val elemSize: Int, val shape: Array[Int], val buffer: ByteBuffer ) {
  def size: Int = shape.product
  def data: Array[Byte] = buffer.array
  def byteSize = shape.product * elemSize
  def byteOffset = offset * elemSize
}

//class CacheFileReader( val datasetFile: String, val varName: String, val sectionOpt: Option[ma2.Section] = None, val cacheType: String = "fragment" ) extends XmlResource {
//  private val netcdfDataset = NetcdfDataset.openDataset( datasetFile )
// private val ncVariable = netcdfDataset.findVariable(varName)

class FileToCacheStream( val cdVariable: CDSVariable, val fragSpec: DataFragmentSpec, val maskOpt: Option[CDByteArray], val cacheType: String = "fragment"  ) extends Loggable {
  private val ncVariable = cdVariable.ncVariable
  private val chunkCache: Cache[Int,CacheChunk] = new FutureCache("Store",cacheType,false)
  private val nReadProcessors = Runtime.getRuntime.availableProcessors - 1
  private val roi: ma2.Section = fragSpec.roi
  private val baseShape = roi.getShape
  private val dType: ma2.DataType  = ncVariable.getDataType
  private val elemSize = ncVariable.getElementSize
  private val range0 = roi.getRange(0)

  def getChunkMemorySize( chunkSize: Int ) : Int = {
    var full_shape = baseShape.clone()
    full_shape(0) = chunkSize
    full_shape.product * elemSize
  }

  def getCacheFilePath: String = {
    val cache_file = "a" + System.nanoTime.toHexString
    DiskCacheFileMgr.getDiskCacheFilePath(cacheType, cache_file)
  }

  def readDataChunks( nChunks: Int, chunkSize: Int, coreIndex: Int ): Int = {
    var subsection = new ma2.Section(roi)

    var nElementsWritten = 0
    for( iChunk <- (coreIndex until nChunks by nReadProcessors); startLoc = iChunk*chunkSize; if(startLoc < baseShape(0)) ) {
      val endLoc = Math.min( startLoc + chunkSize - 1, baseShape(0)-1 )
      val chunkRange = new ma2.Range( startLoc, endLoc )
      subsection.replaceRange(0,chunkRange)
      val data = ncVariable.read(subsection)
      val chunkShape = subsection.getShape
      val chunk = new CacheChunk( startLoc, elemSize, chunkShape, data.getDataAsByteBuffer )
      chunkCache.put( iChunk, chunk )
      nElementsWritten += chunkShape.product
    }
    nElementsWritten
  }

  def execute( chunkSize: Int ): String = {
    val nChunks = if(chunkSize <= 0) { 1 } else { Math.ceil( range0.length / chunkSize.toFloat ).toInt }
    val readProcFuts: IndexedSeq[Future[Int]] = for( coreIndex <- (0 until Math.min( nChunks, nReadProcessors ) ) ) yield Future { readDataChunks(nChunks,chunkSize,coreIndex) }
    writeChunks(nChunks,chunkSize)
  }

  def cacheFloatData( chunkSize: Int  ): CDFloatArray = {
    assert( dType == ma2.DataType.FLOAT, "Attempting to cache %s data as float: %s".format(dType.toString,cdVariable.name) )
    val cache_id = execute( chunkSize )
    getReadBuffer(cache_id) match {
      case (channel, buffer) =>
        val storage: FloatBuffer = buffer.asFloatBuffer
        channel.close
        new CDFloatArray( fragSpec.getShape, storage, cdVariable.missing )
    }
  }

  def getReadBuffer( cache_id: String ): ( FileChannel, MappedByteBuffer ) = {
    val channel = new FileInputStream( cache_id ).getChannel
    channel -> channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size)
  }

  def writeChunks( nChunks: Int, chunkSize: Int ): String = {
    val cacheFilePath = getCacheFilePath
    val chunkByteSize = getChunkMemorySize( chunkSize )
    val channel = new RandomAccessFile(cacheFilePath,"rw").getChannel()
    logger.info( "Writing Buffer file '%s', nChunks = %d, chunkByteSize = %d, size = %d".format( cacheFilePath, nChunks, chunkByteSize, chunkByteSize * nChunks ))
    var buffer: MappedByteBuffer = channel.map( FileChannel.MapMode.READ_WRITE, 0, chunkByteSize * nChunks  )
    (0 until nChunks).foreach( processChunkFromReader( _, buffer ) )
    cacheFilePath
  }

  def processChunkFromReader( iChunk: Int, buffer: MappedByteBuffer ): Unit = {
    chunkCache.get(iChunk) match {
      case Some( cacheChunkFut: Future[CacheChunk] ) =>
        val cacheChunk = Await.result( cacheChunkFut, Duration.Inf )
//        logger.info( "Writing chunk %d, size = %d, position = %d ".format( iChunk, cacheChunk.byteSize, buffer.position ) )
        buffer.put( cacheChunk.data )
      case None =>
        Thread.sleep( 200 )
        processChunkFromReader( iChunk, buffer )
    }
  }


}

//object cacheWriterTest extends App {
//  val data_file = "/usr/local/web/data/MERRA/MERRA300.prod.assim.inst3_3d_asm_Cp.xml"
//  val netcdfDataset = NetcdfDataset.openDataset( data_file )
//  val varName = "t"
//  val ncVariable = netcdfDataset.findVariable(varName)
//  var shape = ncVariable.getShape
//  var section: ma2.Section = new ma2.Section(shape)
//
//
//  println(".")
//}

//dType match {
//  case ma2.DataType.FLOAT =>
//}


