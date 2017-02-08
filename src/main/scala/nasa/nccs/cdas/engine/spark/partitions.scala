package nasa.nccs.cdas.engine.spark
import nasa.nccs.utilities.Loggable
import org.apache.spark.Partitioner
import ucar.nc2.time.CalendarDate

object TimePartitionKey {
  def apply( start: CalendarDate, end: CalendarDate, size: Int ): TimePartitionKey = new TimePartitionKey( start.getMillis, end.getMillis, size )
  def toDate( msecs: Long ): CalendarDate = CalendarDate.of(msecs)
  def toDateStr( msecs: Long ): String = CalendarDate.of(msecs).toString
}

class TimePartitionKey( val startMS: Long, val endMS: Long, val size: Int ) extends Loggable with Serializable with Ordered[TimePartitionKey] {
  import TimePartitionKey._
  val center = (startMS + endMS)/2
  def offset( date: CalendarDate ): Long = center - date.getMillis()
  def compare( that: TimePartitionKey ): Int = startMS.compare( that.startMS )
  def +( that: TimePartitionKey ): TimePartitionKey = {
    if( endMS != that.startMS ) { throw new Exception( s"Attempt to concat non-contiguous partitions: end of first = ${toDateStr(endMS)} <-> start of second = ${toDateStr(that.startMS)}" )}
    new TimePartitionKey( startMS, that.endMS, size + that.size )
  }
  override def toString = s"(${toDateStr(startMS)}<->${toDateStr(endMS)})[${size}]"
}

object TimePartitioner {
  def apply( start: CalendarDate, end: CalendarDate, numParts: Int ): TimePartitioner = new TimePartitioner( start.getMillis, end.getMillis, numParts )
  def apply( keys: Iterable[TimePartitionKey] ): TimePartitioner = {
    val startMS = keys.foldLeft( Long.MaxValue )( ( tval, key ) => Math.min( tval, key.startMS ) )
    val endMS = keys.foldLeft( Long.MinValue )( ( tval, key ) => Math.max( tval, key.endMS ) )
    new TimePartitioner( startMS, endMS, keys.size )
  }
}

class TimePartitioner( val startMS: Long, val endMS: Long, val numParts: Int ) extends Partitioner with Loggable {
  val range: Float = (endMS - startMS).toFloat
  override def numPartitions: Int = numParts
  override def getPartition( key: Any ): Int = {
    val index: Int = key match {
      case tval: TimePartitionKey => (getRelPos(tval) * numParts).toInt
      case wtf => throw new Exception( "Illegal partition key type: " + key.getClass.getName )
    }
    //    logger.info( s" PPPP Get Partition: $index out of $nItems" )
    assert( index < numParts, s"Illegal index value: $index out of $numParts" )
    index
  }
  def getRelPos( tval: TimePartitionKey ): Float = (tval.center - startMS) / range
  override def equals(other: Any): Boolean = other match {
    case tp: TimePartitioner => ( tp.numParts == numParts ) && ( tp.startMS == startMS ) && ( tp.endMS == endMS )
    case _ => false
  }
  def repartition( nParts: Int ): TimePartitioner = new TimePartitioner( startMS, endMS, numParts )
  def startPoint = new TimePartitionKey( startMS, startMS, 0 )
}



//object partTest extends App {
//  val nParts = 17
//  val nItems = 20
//  val partitioner = new IndexPartitioner( nItems, nParts )
//  (0 until nItems) foreach  { index => println( s" $index -> ${partitioner.getPartition(index)}" ) }
//}

