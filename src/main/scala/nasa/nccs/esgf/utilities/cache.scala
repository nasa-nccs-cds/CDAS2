package nasa.nccs.esgf.utilities

import java.io.RandomAccessFile
import java.nio.{FloatBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import nasa.nccs.caching.{Cache, LruCache}
import nasa.nccs.cdapi.cdm.{DiskCacheFileMgr, PartitionedFragment}
import nasa.nccs.cds2.loaders.XmlResource
import nasa.nccs.esgf.process.DataFragmentKey
import ucar.ma2
import ucar.ma2.Range
import ucar.nc2.dataset.NetcdfDataset

import scala.concurrent.duration.Duration

class CacheChunk( offset: Int, shape: Array[Int], data:ma2.Array ) {
  def getSize: Int = shape.product
}

class CacheFileReader( val datasetFile: String, val varName: String, val sectionOpt: Option[ma2.Section] = None, val cacheType: String = "fragment" ) extends XmlResource {
  private val netcdfDataset = NetcdfDataset.openDataset( datasetFile )
  private val ncVariable = netcdfDataset.findVariable(varName)
  private val chunkCache: Cache[Int,CacheChunk] = new LruCache("Store",cacheType,false)
  private val nReadProcessors = Runtime.getRuntime.availableProcessors - 1
  private val roi: ma2.Section = sectionOpt match { case Some(aSection) => aSection; case None => new ma2.Section(ncVariable.getShape) }
  private val baseShape = roi.getShape
  private val dType: ma2.DataType  = ncVariable.getDataType
  private val elemSize = ncVariable.getElementSize
  private val range0 = roi.getRange(0)

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
      val chunk = new CacheChunk(startLoc,chunkShape,data)
      chunkCache.put( iChunk, chunk )
      nElementsWritten += chunkShape.product
    }
    nElementsWritten
  }

  def execute( chunkSize: Int, sectionOpt: Option[ma2.Section] ): Int = {
    val nChunks = Math.ceil( range0.length / chunkSize.toFloat ).toInt
    val readProcFuts: IndexedSeq[Future[Int]] = for( coreIndex <- (0 until nReadProcessors) ) yield Future { readDataChunks(nChunks,chunkSize,coreIndex) }
    val writeProcFut: Future[Int] = Future { writeChunks(nChunks) }
    Await.result( writeProcFut, Duration.Inf )
  }

  def getMemSize( cacheChunk: CacheChunk ): Int = {
    cacheChunk.getSize * elemSize
  }



  def writeChunks(nChunks: Int): Int = {
    val channel = new RandomAccessFile(getCacheFilePath,"rw").getChannel()
    var bufferOpt: Option[MappedByteBuffer] = None
    var nElementsWritten = 0

    for( iChunk <- (0 until nChunks) ) {
      while(true) {
        chunkCache.get(iChunk) match {
          case Some( cacheChunkFut: Future[CacheChunk] ) =>
            val cacheChunk = Await.result( cacheChunkFut, Duration.Inf )
            if( bufferOpt == None )  { bufferOpt = Some( channel.map( FileChannel.MapMode.READ_WRITE, 0, getMemSize( cacheChunk ) ) ) }
            nElementsWritten += writeChunk( cacheChunk, bufferOpt )
           case None => Thread.sleep( 200 )
        }
      }
    }
    nElementsWritten
  }

  def writeChunk( cacheChunk: CacheChunk, bufferOpt: Option[MappedByteBuffer] ): Int = {
    bufferOpt match {
      case Some(buffer) => 0
      case None => throw new Exception( "Error opening Memory Map");
    }
  }

}

object cacheWriterTest extends App {
  val data_file = "/usr/local/web/data/MERRA/MERRA300.prod.assim.inst3_3d_asm_Cp.xml"
  val netcdfDataset = NetcdfDataset.openDataset( data_file )
  val varName = "t"
  val ncVariable = netcdfDataset.findVariable(varName)
  var shape = ncVariable.getShape
  var section: ma2.Section = new ma2.Section(shape)


  println(".")
}

//dType match {
//  case ma2.DataType.FLOAT =>
//}


