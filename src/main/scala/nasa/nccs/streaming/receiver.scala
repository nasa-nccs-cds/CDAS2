package nasa.nccs.streaming
//
//import nasa.nccs.cdapi.tensors.CDFloatArray
//import nasa.nccs.utilities.Loggable
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.receiver.Receiver
//import ucar.nc2.Variable
//import ucar.nc2.dataset.NetcdfDataset
//import ucar.ma2
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.dstream.ReceiverInputDStream


//class SectionFeeder( section: ma2.Section, nRecords: Int, recordSize: Int = 1, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY )
//                                                                                        extends Receiver[String](storageLevel) {
//  def onStart() {
//    new Thread("Feeder Thread") {
//      override def run() { feedSections() }
//    }.start()
//  }
//
//  def onStop() { }
//
//  private def feedSections() = {
//    var startIndex = section.getOrigin(0)
//    val endIndex = startIndex + section.getShape(0)
//    while( ( startIndex < endIndex ) ) {
//      val sections = for( iRecord <- (0 until nRecords); recStart = startIndex + iRecord * recordSize; if recStart < endIndex ) yield {
//        val recLast = Math.min( recStart + recordSize - 1, endIndex -1 )
//        new ma2.Section(section).replaceRange( 0, new ma2.Range( recStart, recLast ) ).toString
//      }
//      store( sections.toIterator )
//      startIndex = startIndex + nRecords * recordSize
//    }
//  }
//}
//
//class SectionReader( val ncmlFile: String, val varName: String ) extends Serializable with Loggable {
//    def read( sectionSpec: String ): CDFloatArray = {
//      try {
//        val datset = NetcdfDataset.openDataset( ncmlFile, true, -1, null, null)
//        Option(datset.findVariable(varName)) match {
//          case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
//          case Some(ncVar) => CDFloatArray.factory( ncVar.read( new ma2.Section(sectionSpec) ), Float.NaN )
//        }
//      } catch {
//        case e: java.io.IOException =>
//          logger.error("Couldn't open dataset %s".format(ncmlFile))
//          throw e
//        case ex: Exception =>
//          logger.error("Something went wrong while reading %s".format(ncmlFile))
//          throw ex
//      }
//    }
//}

//class streamingTest extends Loggable {
//
//  def main(args: Array[String]): Unit = {
//    val ncmlFile = "/att/gpfsfs/ffs2004/ppl/tpmaxwel/cdas/cache/collections/NCML/ncml.xml"
//    val varName = "T"
//    val nRecords = 8
//    val recordSize = 1
//    val conf = new SparkConf().setMaster(s"local[$nRecords]").setAppName("StreamingTest")
//    val ssc = new StreamingContext( conf, Milliseconds(1000) )
//    val section = new ma2.Section( Array(0,10,0,0), Array(53668,1,361,576) )
//    val sectionsStream: ReceiverInputDStream[String] = ssc.receiverStream(new SectionFeeder( section, nRecords, recordSize ) )
//    val sectionReader = new SectionReader( ncmlFile, varName )
//    val inputStream = sectionsStream.map( sectionSpec => sectionReader.read(sectionSpec) )
//    val maxStream = inputStream.map( data => data.max() )
//    maxStream.print(nRecords)
//  }
//}
//
//
