package nasa.nccs.streaming

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.cdapi.data.HeapFltArray
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.esgf.process.CDSection
import nasa.nccs.utilities.Loggable
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import ucar.nc2.Variable
import ucar.nc2.dataset.NetcdfDataset
import ucar.ma2
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.concurrent.Future

object DatasetReader extends Loggable {
  val datasets = new ConcurrentLinkedHashMap.Builder[String, NetcdfDataset].initialCapacity(4).maximumWeightedCapacity(100).build()
  val variables = new ConcurrentLinkedHashMap.Builder[String, Variable].initialCapacity(10).maximumWeightedCapacity(400).build()

  private def getDataset( filePath: String ): NetcdfDataset = datasets.putIfAbsent( filePath, openDataset(filePath) )

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
    variables.putIfAbsent( varPath, findVariable(varPath) )
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
      HeapFltArray(ncVar.read(CDSection(sectionSpec).toSection), Array(0, 0, 0, 0), "", Map.empty[String, String], Float.NaN)
    }
}

object DataProcessor extends Loggable {
  def apply( data: HeapFltArray ): Float = {
    val result = data.toCDFloatArray.max().getStorageValue(0)
    logger.info( "DataProcessor computing max: " + result )
    result
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
    val section = new CDSection( Array(0,10,0,0), Array(53668,1,361,576) )
    val sectionsStream: ReceiverInputDStream[String] = ssc.receiverStream(new SectionFeeder( section, nRecords, recordSize ) )
    val sectionReader = new SectionReader( ncmlFile, varName )
    val inputStream = sectionsStream.map( sectionSpec => sectionReader.read(sectionSpec) )
    val maxStream = inputStream.map( DataProcessor(_) )
    maxStream.foreachRDD( rdd => println( "------>>>> Result: " + rdd.collect().mkString(", ") ) )
    ssc.start()
    ssc.awaitTermination()
  }
}


