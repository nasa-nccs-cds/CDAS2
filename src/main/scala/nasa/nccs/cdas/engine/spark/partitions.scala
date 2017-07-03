package nasa.nccs.cdas.engine.spark
import nasa.nccs.cdapi.data.RDDRecord$
import nasa.nccs.utilities.Loggable
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import ucar.nc2.time.CalendarDate

object LongRange {
  type StrRep = (Long) => String
  implicit def ordering[A <: LongRange]: Ordering[A] = Ordering.by(_.start)
  def apply( start: Long, end: Long ): LongRange = new LongRange( start, end )

  def apply( ranges: Iterable[LongRange] ): LongRange = {
    val startMS = ranges.foldLeft( Long.MaxValue )( ( tval, key ) => Math.min( tval, key.start ) )
    val endMS = ranges.foldLeft( Long.MinValue )( ( tval, key ) => Math.max( tval, key.end) )
    new LongRange( startMS, endMS )
  }
  implicit val strRep: LongRange.StrRep = _.toString
}

class LongRange(val start: Long, val end: Long ) extends Serializable {
  import LongRange._
  def this( r: LongRange ) { this( r.start, r.end ) }
  def center = (start + end)/2
  val size: Double = (end - start).toDouble
  override def equals(other: Any): Boolean = other match {
    case tp: LongRange => ( tp.start == start) && ( tp.end == end)
    case _ => false
  }
  override def hashCode() = (( start + end ) % Int.MaxValue).toInt
  def compare( that: LongRange ): Int = start.compare( that.start )
  def getRelPos( location: Long ): Double =
    (location - start) / size
  def intersect( other: LongRange ): Option[LongRange] = {
    val ( istart, iend ) = ( Math.max( start, other.start ),  Math.min( end, other.end ) )
    if ( istart <= iend ) Some( new LongRange( istart, iend ) ) else None
  }
  def disjoint( other: LongRange ) = ( end < other.start ) || ( other.end < start )
  def merge( other: LongRange ): Option[LongRange] = if( disjoint(other) ) None else Some( union(other) )
  def union( other: LongRange ): LongRange = new LongRange( Math.min( start, other.start ), Math.max( end, other.end ) )

  def +( that: LongRange ): LongRange = LongRange( Math.min(start, that.start), Math.max( end, that.end ) )
  def +!( that: LongRange ): LongRange = {
    if( end != that.start ) { throw new Exception( s"Attempt to concat non-contiguous ranges: first = ${toString} <-> second = ${that.toString}" )}
    LongRange( start, that.end )
  }
  def contains( loc: Long ): Boolean = ( loc >= start ) && ( loc < end )
  def locate( loc: Long ): Int = if ( loc < start ) -1 else if ( (loc >= end) && (end > start) ) 1 else 0
  def print( implicit strRep: StrRep ) = s"${strRep(start)}<->${strRep(end)}"
  override def toString = print
}

object RecordKey {
  def apply( start: Long, end: Long, elemStart: Int, numElems: Int ): RecordKey = new RecordKey( start, end, elemStart, numElems )

  def apply( ranges: Iterable[RecordKey] ): RecordKey = {
    val startMS = ranges.foldLeft( Long.MaxValue )( ( tval, key ) => Math.min( tval, key.start ) )
    val endMS = ranges.foldLeft( Long.MinValue )( ( tval, key ) => Math.max( tval, key.end) )
    val nElems = ranges.foldLeft( 0 )( ( tval, key ) => tval + key.numElems )
    new RecordKey( startMS, endMS, ranges.head.elemStart, nElems )
  }
  implicit val strRep: LongRange.StrRep = _.toString
  val empty = RecordKey(0L,0L,0,0)

}

class RecordKey(start: Long, end: Long, val elemStart: Int, val numElems: Int ) extends LongRange( start, end ) with Ordered[RecordKey] with Serializable with Loggable {
  import LongRange._
  override def equals(other: Any): Boolean = other match {
    case tp: RecordKey => ( tp.start == start) && ( tp.end == end) && ( tp.numElems == numElems) && ( tp.elemStart == elemStart )
    case lr: LongRange => ( lr.start == start ) && ( lr.end == end )
    case _ => false
  }
  def singleElementKey( relIndex: Int ) = {
    val range = end-start;
    val start1 = start + math.round( range * relIndex / (numElems).toFloat )
    val end1 = start + math.round( range * (relIndex+1) / (numElems).toFloat )
    RecordKey( start1, end1, elemStart + relIndex, 1 )
  }
  override def hashCode() = ( (start + end + elemStart + numElems) % Int.MaxValue ).toInt
  def elemEnd = elemStart + numElems
  def sameRange( lr: LongRange ): Boolean = ( lr.start == start ) && ( lr.end == end )
  def estElemIndexAtLoc( loc: Long ): Int =  ( elemStart + getRelPos(loc) * numElems ).toInt
  def compare( that: RecordKey ): Int = start.compare( that.start )
  def elemRange: (Int,Int) = ( elemStart, elemStart + numElems )

  def +( that: RecordKey ): RecordKey = if( empty ) {that} else if( that.empty ) { this } else {
    RecordKey( Math.min(start,that.start), Math.max(end,that.end), Math.min(elemStart,that.elemStart), numElems + that.numElems )
  }

  def +!( that: RecordKey ): RecordKey = if( empty ) {that} else if( that.empty ) { this } else {
    if( (end != that.start) || (elemEnd != that.elemStart) ) { throw new Exception( s"Attempt to concat non-contiguous partition keys: first = ${toString} <-> second = ${that.toString}" )}
    RecordKey( start, that.end, elemStart, numElems + that.numElems )
  }
  def startPoint: RecordKey  = RecordKey( start, start, elemStart, 0 )

  def intersect( other: RecordKey ): Option[RecordKey] = {
    val ( istart, iend ) = ( Math.max( start, other.start ),  Math.min( end, other.end ) )
    if ( istart <= iend ) {
      val ( ielemStart, ielemEnd ) = ( Math.max( elemStart, other.elemStart ),  Math.min( elemEnd, other.elemEnd ) )
      val result = Some( RecordKey( istart, iend, ielemStart, ielemEnd-ielemStart ) )
      logger.debug( s"  @@ PartitionKey Intersect: ${toString} + ${other.toString} => ${result.toString} ")
      result
    }  else {
      logger.debug( s"  @@ PartitionKey **NO** Intersect: ${toString} + ${other.toString} ")
      None
    }
  }
  override def print( implicit strRep: StrRep ) = s"{ ${strRep(start)}<->${strRep(end)}, ${elemStart}<->${elemEnd} }"
  override def toString() = s"Key[ ${strRep(start)}<->${strRep(end)}, ${elemStart}<->${elemEnd} ]"
  def empty = ( numElems == 0 )
}

object RangePartitioner {
  def apply( partitions: Map[Int,RecordKey] ): RangePartitioner = {
    new RangePartitioner( partitions )
  }
  def apply( ranges: Iterable[RecordKey] ): RangePartitioner = {
    val indexed_ranges = ranges.zipWithIndex map { case (range, index) => index -> range }
    new RangePartitioner( Map( indexed_ranges.toSeq: _* ) )
  }
//  def apply( range: LongRange, nParts: Int ): RangePartitioner = {
//    val psize: Double = range.size / nParts
//    val parts = Map( ( 0 until nParts ) map (index => index -> LongRange( ( range.start + index * psize ).toLong, ( range.start + ( index + 1 ) * psize ).toLong ) ): _* )
//    new RangePartitioner(parts)
//  }

}


class RangePartitioner( val partitions: Map[Int,RecordKey] ) extends Partitioner with Loggable {
  val range = RecordKey(partitions.values)
  val numParts = partitions.size
  val numElems = range.numElems
  val psize: Double = range.size / numParts
  override def numPartitions: Int = numParts

  def getCoordRangeMap = {
    val startIndices =  partitions.mapValues( _.elemRange )
    startIndices
  }

  override def equals( other: Any ): Boolean = other match {
    case partitioner: RangePartitioner =>
      ( numParts == partitioner.numParts ) && ( numElems == partitioner.numElems ) && !differentPartitions( partitioner.partitions )
    case x => super.equals(x)
  }

  def differentPartitions( parts: Map[Int,RecordKey] ): Boolean =
    partitions.exists { case (index,partkey) => parts.get(index) match { case Some(partkey1) => !partkey1.equals(partkey); case None => true } }

  def findPartIndex( index: Int, loc: Long ): Int = partitions.get(index) match {
    case None => if( loc == range.end ) numParts-1 else -1
    case Some( key ) => key.locate( loc ) match {
      case 0 => index
      case x => findPartIndex( index + x, loc )
    }
  }

  def intersect( key: RecordKey ): IndexedSeq[RecordKey]= {
    val (startIndex, endIndex) = (getPartIndexFromLocation(key.start), getPartIndexFromLocation(key.end-1))
    val partKeys = (startIndex to endIndex) flatMap ( index => partitions.get(index) )
    partKeys flatMap ( partkey => partkey.intersect(key) )
  }

  def estPartIndexAtLoc( loc: Long ): Int = ( range.getRelPos(loc) * numParts ).toInt

  def colaesce: RangePartitioner = RangePartitioner( List( range ) )

  def getPartIndexFromLocation( loc: Long ) =
    findPartIndex( estPartIndexAtLoc( loc ), loc )

  override def getPartition( key: Any ): Int = {
    val index: Int = key match {
      case rkey: LongRange => getPartIndexFromLocation(rkey.center)
      case wtf => throw new Exception( "Illegal partition key type: " + key.getClass.getName )
    }
    if( index >= numParts )
      throw new Exception( s"Illegal index value: $index out of $numParts for key ${key.toString}" )
    if( index < 0 )
      throw new Exception( s"Can't find partition index for key ${key.toString}" )
    index
  }

  def newPartitionKey( irange: LongRange ): Option[RecordKey] = irange.intersect( range ) flatMap (keyrange => partitions.get( getPartIndexFromLocation(keyrange.center) ) )
  def newPartitionKeyOpt( location: Long ): Option[RecordKey] = partitions.get( getPartIndexFromLocation(location) )
  def newPartitionKey( location: Long ): RecordKey = partitions.get( getPartIndexFromLocation(location) ) getOrElse( throw new Exception( s"Location of new key ${location} is out of bounds for partitioner"))
}

//object PartitionManager {
//
//  def getPartitioner( rdd: RDD[(LongRange,RDDPartition)], nParts: Int = -1 ): RangePartitioner = {
//    rdd.partitioner match {
//      case Some( partitioner ) => partitioner match {
//        case range_partitioner: RangePartitioner => if( nParts > 0 ) range_partitioner.repartition(nParts) else range_partitioner
//        case wtf => throw new Exception( "Found partitioner of wrong type: " + wtf.getClass.getName )
//      }
//      case None => throw new Exception( "Missing partitioner for rdd"  )
//    }
//  }
//}



//object partTest extends App {
//  val nParts = 17
//  val nItems = 20
//  val partitioner = new IndexPartitioner( nItems, nParts )
//  (0 until nItems) foreach  { index => println( s" $index -> ${partitioner.getPartition(index)}" ) }
//}

