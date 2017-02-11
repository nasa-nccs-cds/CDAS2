package nasa.nccs.cdas.engine.spark
import nasa.nccs.cdapi.data.RDDPartition
import nasa.nccs.utilities.Loggable
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import ucar.nc2.time.CalendarDate

object LongRange {
  type StrRep = (Long) => String
  def apply( start: Long, end: Long ): LongRange = new LongRange( start, end )
}
class LongRange(val start: Long, val end: Long ) extends Ordered[LongRange] {
  def this( r: LongRange ) { this( r.start, r.end ) }
  def center = (start + end)/2
  val size: Double = (end - start).toDouble
  override def equals(other: Any): Boolean = other match {
    case tp: LongRange => ( tp.start == start) && ( tp.end == end)
    case _ => false
  }
  def compare( that: LongRange ): Int = start.compare( that.start )
  def getRelPos( location: Long ): Double = (location - start) / size
  def intersect( other: LongRange ): Option[LongRange] = {
    val ( istart, iend ) = ( Math.max( start, other.start ),  Math.min( end, other.end ) )
    if ( istart <= iend ) Some( new LongRange( istart, iend ) ) else None
  }
  def disjoint( other: LongRange ) = ( end < other.start ) || ( other.end < start )
  def merge( other: LongRange ): Option[LongRange] = if( disjoint(other) ) None else Some( union(other) )
  def union( other: LongRange ): LongRange = new LongRange( Math.min( start, other.start ), Math.max( end, other.end ) )
  def +( that: LongRange ): LongRange = {
    if( end != that.start ) { throw new Exception( s"Attempt to concat non-contiguous partitions: first = ${toString} <-> second = ${that.toString}" )}
    LongRange( start, that.end )
  }
  def startPoint: LongRange  = LongRange( start, start )
  def print( implicit strRep: LongRange.StrRep ) = s"${strRep(start)}<->${strRep(end)}"
  override def toString = print
}

object TimePartitioner {
  def strRep( value: Long ): String = CalendarDate.of(value).toString
}

class RangePartitioner( val range: LongRange, val numParts: Int) extends Partitioner with Loggable {
  val psize: Double = range.size / numParts
  val partitions: Map[Int,LongRange] = getPartitions
  override def numPartitions: Int = numParts
  override def getPartition( key: Any ): Int = {
    val index: Int = key match {
      case rkey: LongRange => getPartIndexFromLocation(rkey.center)
      case wtf => throw new Exception( "Illegal partition key type: " + key.getClass.getName )
    }
    assert( index < numParts, s"Illegal index value: $index out of $numParts" )
    index
  }
  def intersect( other: RangePartitioner, nParts: Int = numParts ): Option[RangePartitioner] = range.intersect(other.range) map( lrange => new RangePartitioner( lrange, nParts ) )
  def union( other: RangePartitioner, nParts: Int = numParts ): RangePartitioner =  new RangePartitioner( range.union(other.range), nParts )
  def getPartIndexFromLocation( loc: Long ) = ( range.getRelPos(loc) * numParts ).toInt
  def repartition( nParts: Int ): RangePartitioner = new RangePartitioner(range, nParts)
  def pstart( index: Int ): Long = ( range.start + index * psize ).toLong
  def pend( index: Int ): Long = ( range.start + ( index + 1 ) * psize ).toLong
  def getPartitionRange( index: Int ): LongRange = LongRange( pstart(index), pend(index) )
  def getPartitions: Map[Int,LongRange] = Map( ( 0 until numParts ) map (index => index -> getPartitionRange(index) ): _* )
  def newPartitionKey( irange: LongRange ): Option[LongRange] = irange.intersect( range ) flatMap ( keyrange => partitions.get( getPartIndexFromLocation(keyrange.center) ) )
  def newPartitionKey( location: Long ): Option[LongRange] = partitions.get( getPartIndexFromLocation(location) )
}

object PartitionManager {

  def getPartitioner( rdd: RDD[(LongRange,RDDPartition)], nParts: Int = -1 ): RangePartitioner = {
    rdd.partitioner match {
      case Some( partitioner ) => partitioner match {
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

