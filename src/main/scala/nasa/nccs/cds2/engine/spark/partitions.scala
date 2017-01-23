package nasa.nccs.cds2.engine.spark
import nasa.nccs.utilities.Loggable
import org.apache.spark.Partitioner


class IndexPartitioner( val nItems: Int, val numParts: Int ) extends Partitioner with Loggable {

//  logger.info( s" PPPP Partition: ( $nItems $numParts ): \n " + Thread.currentThread().getStackTrace().mkString("\n"))
  override def numPartitions: Int = numParts
  def scale = numParts / nItems.toFloat
  override def getPartition( key: Any ): Int = {
    val index = key match {
      case ival: Int => ival
      case sval: String => sval.toInt
      case wtf => throw new Exception( "Illegal partition key type: " + key.getClass.getName )
    }
//    logger.info( s" PPPP Get Partition: $index out of $nItems" )
    assert( index < nItems, s"Illegal index value: $index out of $nItems" )
    if( nItems <= numParts ) index else (index * scale).toInt
  }
  override def equals(other: Any): Boolean = other match {
     case tp: IndexPartitioner => ( tp.numParts == numParts ) && ( tp.nItems == nItems )
     case _ => false
   }
}


/*
object partTest extends App {
  val nParts = 17
  val nItems = 20
  val partitioner = new IndexPartitioner( nItems, nParts )
  (0 until nItems) foreach  { index => println( s" $index -> ${partitioner.getPartition(index)}" ) }
}
*/
