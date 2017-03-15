package nasa.nccs.streaming

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.cdapi.data.HeapFltArray
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.esgf.process.CDSection
import nasa.nccs.streaming.DataProcessor.logger
import nasa.nccs.streaming.DatasetReader.logger
import nasa.nccs.streaming.streamingTest.logger
import nasa.nccs.utilities.Loggable
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import ucar.nc2.Variable
import ucar.nc2.dataset.NetcdfDataset
import ucar.ma2
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import scala.concurrent.Future

object DatasetReader extends Loggable {
  val datasets = new ConcurrentLinkedHashMap.Builder[String, NetcdfDataset].initialCapacity(4).maximumWeightedCapacity(100).build()
  val variables = new ConcurrentLinkedHashMap.Builder[String, Variable].initialCapacity(10).maximumWeightedCapacity(400).build()

  private def getDataset( filePath: String ): NetcdfDataset = {
    if( !datasets.containsKey(filePath) ) { datasets.put( filePath, openDataset(filePath) ) }
    datasets.get( filePath )
  }

  private def openDataset( filePath: String ): NetcdfDataset = {
    try {
      NetcdfDataset.openDataset( filePath, true, -1, null, null)
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(filePath))
        throw e
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(filePath))
        throw ex
    }
  }

  def getVariable( filePath: String, varName: String ): Variable = {
    val varPath = filePath + "|" + varName
    if( !variables.containsKey(varPath) ) { variables.put( varPath, findVariable(varPath) ) }
    variables.get( varPath )
  }

  private def findVariable( varPath: String ): Variable = {
    val toks = varPath.split('|')
    val ( filepath, varName ) = ( toks(0), toks(1) )
    val dataset = getDataset( filepath )
    Option(dataset.findVariable(varName)) match {
      case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
      case Some(ncVar) => ncVar
    }
  }
}

class SectionFeeder( section: CDSection, nRecords: Int, recordSize: Int = 1, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY )
                                                                                        extends Receiver[String](storageLevel) {
  def onStart() {
    new Thread("Feeder Thread") {
      override def run() { feedSections() }
    }.start()
  }

  def onStop() { }

  private def feedSections() = {
    var startIndex = section.getOrigin(0)
    val endIndex = startIndex + section.getShape(0)
    while( ( startIndex < endIndex ) ) {
      val sections = for( iRecord <- (0 until nRecords); recStart = startIndex + iRecord * recordSize; if recStart < endIndex ) yield {
        val recEnd = Math.min( recStart + recordSize, endIndex )
        section.subserialize( 0, recStart, recEnd-recStart )
      }
      store( sections.toIterator )
      startIndex = startIndex + nRecords * recordSize
    }
  }
}

class SectionReader( val ncmlFile: String, val varName: String ) extends Serializable with Loggable {
    def read( sectionSpec: String ): HeapFltArray = {
      val ncVar = DatasetReader.getVariable(ncmlFile, varName)
      try {
        val data = ncVar.read(CDSection(sectionSpec).toSection)
        logger.info( "SectionReader accessing data for sectionSpec: " + sectionSpec )
        HeapFltArray(data, Array(0, 0, 0, 0), "", Map.empty[String, String], Float.NaN)
      } catch {
        case e: Exception =>
          logger.error("Error reading variable %s with section spec %s: %s".format( varName, sectionSpec, e.toString ))
          HeapFltArray.empty(4)
      }
    }
}

object DataProcessor extends Loggable {
  var currentTime = 0L
  def apply( data: HeapFltArray ): Float = {
    val result = computeMax(data)
    if( currentTime > 0L ) { println("Elapsed batch time = %.4f sec, thread = %d".format( (System.nanoTime() - currentTime) / 1.0E9, Thread.currentThread().getId )) }
    currentTime = System.nanoTime()
    result
  }
  def computeMax( data: HeapFltArray ): Float = {
    val t0 = System.nanoTime()
    var max = Float.MinValue
    val datasize = data.shape.size
    for( index <- 0 until datasize; dval = data.data(index); if !dval.isNaN ) { max = Math.max(max, dval) }
    if (max == Float.MinValue) max = Float.NaN
    logger.info( "DataProcessor computing max: %s, time = %.4f sec, thread = %d".format( max.toString, (System.nanoTime() - currentTime) / 1.0E9, Thread.currentThread().getId))
    max
  }
}

object DataLogger extends Loggable {
  var currentTime = 0L
  def apply( rdd: RDD[Float] ): Unit = {
    logger.info( "------>>>> Result: " + rdd.collect().mkString(", ") )
    if( currentTime > 0L ) { println("Elapsed batch time = %.4f sec".format( (System.nanoTime() - currentTime) / 1.0E9)) }
    currentTime = System.nanoTime()
  }
}

object streamingTest extends Loggable {

  def main(args: Array[String]): Unit = {
    val ncmlFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/npana.xml"
    val varName = "T"
    val nRecords = 8
    val recordSize = 1
    val conf = new SparkConf().setMaster(s"local[$nRecords]").setAppName("StreamingTest")
    val ssc = new StreamingContext( conf, Milliseconds(1000) )
    ssc.sparkContext.setLogLevel("WARN" )
    val section = new CDSection( Array(0,10,0,0), Array(53668,1,361,576) )
    val sectionsStream: ReceiverInputDStream[String] = ssc.receiverStream(new SectionFeeder( section, nRecords, recordSize ) )
    val sectionReader = new SectionReader( ncmlFile, varName )
    val inputStream: DStream[HeapFltArray] = sectionsStream.map( sectionSpec => sectionReader.read(sectionSpec) )
    val maxStream: DStream[Float] = inputStream.map { DataProcessor(_) }
    maxStream.foreachRDD { DataLogger(_)  }
    ssc.start()
    ssc.awaitTermination()
  }
}


