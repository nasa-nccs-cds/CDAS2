package nasa.nccs.cdas.engine.spark
import nasa.nccs.cdapi.data.RDDPartition
import nasa.nccs.utilities.Loggable
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import ucar.nc2.time.CalendarDate

class RangePartitionKey(val start: Long, val end: Long, val size: Int) extends Loggable with Serializable with Ordered[RangePartitionKey] {
  val center = (start + end)/2
  def offset( pos: Long ): Long = center - pos
  def compare( that: RangePartitionKey ): Int = start.compare( that.start )
  def +( that: RangePartitionKey ): RangePartitionKey = {
    if( end != that.start ) { throw new Exception( s"Attempt to concat non-contiguous partitions: first = ${toString} <-> second = ${that.toString}" )}
    alloc(start, that.end, size + that.size)
  }
  override def toString = s"(${posStr(start)}<->${posStr(end)})[${size}]"
  def posStr( pos: Long ): String = pos.toString
  def alloc( start: Long, end: Long, size: Int ): RangePartitionKey = new RangePartitionKey(start, end, size)
}

object TimePartitionKey {
  def apply( start: CalendarDate, end: CalendarDate, size: Int ): TimePartitionKey = new TimePartitionKey(start.getMillis, end.getMillis, size)
}

class TimePartitionKey( start: Long, end: Long, size: Int) extends RangePartitionKey( start, end, size ) {
  override def posStr( pos: Long ): String = CalendarDate.of(pos).toString
  def posDate( pos: Long ): CalendarDate = CalendarDate.of(pos)
  override def alloc( start: Long, end: Long, size: Int ): TimePartitionKey = new TimePartitionKey(start, end, size)
}

object TimePartitioner {
  def apply( start: CalendarDate, end: CalendarDate, numParts: Int ): RangePartitioner = new RangePartitioner(start.getMillis, end.getMillis, numParts)
  def apply( keys: Iterable[RangePartitionKey] ): RangePartitioner = {
    val startMS = keys.foldLeft( Long.MaxValue )( ( tval, key ) => Math.min( tval, key.start ) )
    val endMS = keys.foldLeft( Long.MinValue )( ( tval, key ) => Math.max( tval, key.end) )
    new TimePartitioner(startMS, endMS, keys.size)
  }
}

class RangePartitioner(val start: Long, val end: Long, val numParts: Int) extends Partitioner with Loggable {
  val range: Float = (end - start).toFloat
  override def numPartitions: Int = numParts
  override def getPartition( key: Any ): Int = {
    val index: Int = key match {
      case tval: RangePartitionKey => (getRelPos(tval) * numParts).toInt
      case wtf => throw new Exception( "Illegal partition key type: " + key.getClass.getName )
    }
    //    logger.info( s" PPPP Get Partition: $index out of $nItems" )
    assert( index < numParts, s"Illegal index value: $index out of $numParts" )
    index
  }
  def getRelPos( tval: RangePartitionKey ): Float = (tval.center - start) / range
  override def equals(other: Any): Boolean = other match {
    case tp: RangePartitioner => ( tp.numParts == numParts ) && ( tp.start == start) && ( tp.end == end)
    case _ => false
  }
  def repartition( nParts: Int ): RangePartitioner = alloc(start, end, nParts)
  def startPoint = key(start, start, 0)
  def alloc( start: Long, end: Long, nParts: Int ) = new RangePartitioner(start, end, nParts)
  def key( start: Long, end: Long, size: Int ) = new RangePartitionKey(start, end, size)
}

class TimePartitioner( start: Long, end: Long, numParts: Int) extends RangePartitioner( start, end, numParts ) {
  override def alloc( start: Long, end: Long, nParts: Int ) = new TimePartitioner(start, end, nParts)
  override def key( start: Long, end: Long, size: Int ) = new TimePartitionKey(start, end, size)
}
object PartitionManager {

  def getPartitioner( rdd: RDD[(RangePartitionKey,RDDPartition)], nParts: Int = -1 ): RangePartitioner = {
    rdd.partitioner match {
      case Some( partitioner ) => partitioner match {
        case time_partitioner: TimePartitioner => if( nParts > 0 ) time_partitioner.repartition(nParts) else time_partitioner
        case range_partitioner: RangePartitioner => if( nParts > 0 ) range_partitioner.repartition(nParts) else range_partitioner
        case wtf => throw new Exception( "Found partitioner of wrong type: " + wtf.getClass.getName )
      }
      case None => throw new Exception( "Missing partitioner for rdd"  )
    }
  }

}



//object partTest extends App {
//  val nParts = 17
//  val nItems = 20
//  val partitioner = new IndexPartitioner( nItems, nParts )
//  (0 until nItems) foreach  { index => println( s" $index -> ${partitioner.getPartition(index)}" ) }
//}

