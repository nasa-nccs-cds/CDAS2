// Based on ucar.ma2.Index, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

package nasa.nccs.cdapi.tensors
import java.util.Formatter

import ucar.nc2.time.Calendar
import ucar.nc2.time.CalendarDate
import nasa.nccs.cdapi.cdm.CDSVariable
import nasa.nccs.esgf.process.{DomainAxis, GridContext, GridCoordSpec, TargetGrid}
import nasa.nccs.utilities.{Loggable, cdsutils}

import scala.collection.mutable.ListBuffer
import ucar.ma2
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis1D, CoordinateAxis1DTime}
import ucar.nc2.time.CalendarPeriod.Field._
import ucar.nc2.time.{Calendar, CalendarDate}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CDIndexMap {

  def apply(index: CDIndexMap): CDIndexMap = new CDIndexMap(index.getShape, index.getStride, index.getOffset )
  def apply( shape: Array[Int], stride: Array[Int]=Array.emptyIntArray, offset: Int = 0, coordMaps: Map[Int,CDCoordMap] = Map.empty): CDIndexMap = new CDIndexMap(shape, stride, offset, coordMaps )
  def apply( shape: Array[Int], coordMap: Map[Int,CDCoordMap] ): CDIndexMap = new CDIndexMap( shape, Array.emptyIntArray, 0, coordMap )
  def apply( shape: Array[Int] ): CDIndexMap = new CDIndexMap( shape, Array.emptyIntArray, 0, Map.empty[Int,CDCoordMap] )
  def apply( shape: Array[Int], coordMapList: List[CDCoordMap] ): CDIndexMap = {
    val coordMaps = coordMapList.map( cmap => cmap.dimension -> cmap )
    new CDIndexMap( shape, Array.emptyIntArray, 0, Map(coordMaps:_*) )
  }
  def const(shape: Array[Int]): CDIndexMap = new CDIndexMap(shape, Array.fill[Int](shape.length)(0), 0 )
  def empty = apply( Array.emptyIntArray )
}

abstract class IndexMapIterator extends collection.Iterator[Int] {
  val _length: Int = getLength
  var count = 0
  def hasNext: Boolean = { count < _length }
  def next(): Int = { val rv = getValue(count); count=count+1; rv }
  def getValue( index: Int ): Int
  def getLength: Int
}

abstract class TimeIndexMapIterator( val timeOffsets: Array[Long], range: ma2.Range  ) extends IndexMapIterator {
  val index_offset: Int = range.first()
  val timeHelper = new ucar.nc2.dataset.CoordinateAxisTimeHelper( Calendar.gregorian, cdsutils.baseTimeUnits )
  override def getLength: Int =  range.last() - range.first() + 1
  def toDate( cd: CalendarDate ): DateTime = new DateTime( cd.toDate )
  def getCalendarDate( index: Int ) = timeHelper.makeCalendarDateFromOffset( timeOffsets(index) )
}

class YearOfCenturyIter( timeOffsets: Array[Long], range: ma2.Range ) extends TimeIndexMapIterator(timeOffsets,range) {
  def getValue( count_index: Int ): Int = toDate( getCalendarDate(count_index+index_offset) ).getYearOfCentury
}

class MonthOfYearIter( timeOffsets: Array[Long], range: ma2.Range  ) extends TimeIndexMapIterator(timeOffsets,range) {
  def getValue( count_index: Int ): Int = {
    val rdate = toDate( getCalendarDate(count_index + index_offset) )
    val dom = rdate.getDayOfMonth
    if( dom < 22 ) {
      rdate.getMonthOfYear - 1
    } else {
      rdate.getMonthOfYear
//      val doy = rdate.getDayOfYear - 1
//      ((doy / 365.0) * 12.0).round.toInt % 12
    }
  }
}

class DayOfYearIter( timeOffsets: Array[Long], range: ma2.Range ) extends TimeIndexMapIterator(timeOffsets,range) {
  def getValue( count_index: Int ): Int = toDate( getCalendarDate(count_index+index_offset) ).getDayOfYear
}

class CDIndexMap( protected val shape: Array[Int], _stride: Array[Int]=Array.emptyIntArray, protected val _offset: Int = 0, protected val _coordMaps: Map[Int,CDCoordMap] = Map.empty ) extends Serializable with Loggable {
  protected val rank: Int = shape.length
  protected val stride = if( _stride.isEmpty ) computeStrides(shape) else _stride
  def this( index: CDIndexMap ) = this( index.shape, index.stride, index._offset, index._coordMaps )
  def getCoordMaps: List[CDCoordMap] = _coordMaps.values.toList
  def getCoordMap: Map[Int,CDCoordMap] = _coordMaps

  def append( other: CDIndexMap ): CDIndexMap = {
    if( shape.contains(0) ) { other }
    else if( other.shape.contains(0) ) { this }
    else {
      for (i <- (1 until rank)) if (shape(i) != other.shape(i)) throw new Exception("Can't merge arrays with non-commensurate shapes: %s vs %s".format(shape.mkString(","), other.shape.mkString(",")))
      assert((_offset == 0) || (other._offset == 0), "Can't merge subsetted arrays, should extract section first.")
      val newShape: IndexedSeq[Int] = for (i <- (0 until rank)) yield if (i == 0) shape(i) + other.shape(i) else shape(i)
      new CDIndexMap(newShape.toArray, _stride, _offset, _coordMaps)
    }
  }
  def isStorageCongruent(storageSize: Int): Boolean = ( getSize == storageSize ) && !broadcasted &&  _coordMaps.isEmpty   // isStorageCongruent(getStorageSize)
  def getRank: Int = rank
  def getShape: Array[Int] = ( for( idim <- (0 until rank) ) yield _coordMaps.get(idim).map( _.mapArray.length ).getOrElse(shape( idim )) ).toArray
  def getStride: Array[Int] = stride.clone
  def getShape(index: Int): Int = shape(index)
  def getSize: Int = if( rank == 0 ) { 0 } else { shape.filter( _ > 0 ).product }
  def getOffset: Int = _offset

  def getStorageShape: Array[Int] = {
    val shape_seq = for (idim <- (0 until rank)) yield
      _coordMaps.get(idim) match {
        case Some(cmap) => cmap.nBins
        case None => if (stride(idim) == 0) 1 else shape(idim)
      }
    shape_seq.toArray
  }

  override def toString: String = "{ Shape: " + shape.mkString("[ ",", "," ], Stride: " + stride.mkString("[ ",", "," ]") + " Offset: " + _offset + " } ")

  def broadcasted: Boolean = {
    for( i <- (0 until rank) ) if( (stride(i) == 0) && (shape(i) > 1) ) return true
    false
  }

  def getCoordIndices( flatIndex: Int ): IndexedSeq[Int] = {
    var currElement = flatIndex
    currElement -= _offset
    for( ii <-(0 until rank ) ) yield if (shape(ii) < 0) {  -1 } else {
      val coordIndex = currElement / stride(ii)
      currElement -= coordIndex * stride(ii)
      coordIndex.toInt
    }
  }

  def getStorageIndex( coordIndices: Array[Int] ): Int = {
    assert( coordIndices.length == rank, "Wrong number of coordinates in getStorageIndex for Array of rank %d: %d".format( rank, coordIndices.length) )
    var value: Int = _offset
    for( ii <-(0 until rank ); if (shape(ii) >= 0)  ) _coordMaps.get(ii) match {
      case Some(cmap) =>
        value += cmap.mapArray( coordIndices(ii) ) * stride(ii)
      case None =>
        value += coordIndices(ii) * stride(ii)
    }
    value
  }

  def computeStrides( shape: Array[Int] ): Array[Int] = {
    var product: Int = 1
    var strides = for (ii <- (shape.length - 1 to 0 by -1); thisDim = shape(ii) ) yield {
      val cmap = _coordMaps.get(ii)
      if ( (thisDim > 1) || cmap.isDefined ) {
        val curr_stride = product
        product *= cmap.map(_.nBins).getOrElse(thisDim)
        curr_stride
      } else {
        0
      }
    }
    return strides.reverse.toArray
  }

  def getAccumulator( reduceDims: Array[Int], optCoordMap: Option[CDCoordMap]  ): CDIndexMap = getAccumulator( reduceDims, optCoordMap.toList )
  def getAccumulator( reduceDims: Array[Int], coordMaps: List[CDCoordMap]  ): CDIndexMap = getAccumulator( reduceDims, Map( coordMaps.map(coordMap => coordMap.dimension -> coordMap ):_*) )

  def getAccumulator( reduceDims: Array[Int], coordMaps: Map[Int,CDCoordMap] = Map.empty  ): CDIndexMap = {
    val cMaps = coordMaps ++ _coordMaps
    val full_shape = getShape
    val new_shape: IndexedSeq[Int] = for( ii <-(0 until rank )  ) yield cMaps.get(ii) match {
      case Some(cmap) => cmap.nBins
      case None => if( reduceDims.contains(ii) || reduceDims.isEmpty ) 1 else full_shape(ii)
    }
    val rv = CDIndexMap( new_shape.toArray, cMaps ).broadcast( full_shape )
    rv
  }

//  def flip(index: Int): CDIndexMap = {
//    assert ( (index >= 0) && (index < rank), "Illegal rank index: " +  index )
//    val new_index = if (shape(index) >= 0) {
//      val new_offset = _offset + stride(index) * (shape(index) - 1)
//      val new_stride = stride.clone
//      new_stride(index) = -stride(index)
//      new CDIndexMap( shape, new_stride, new_offset )
//    } else new CDIndexMap( this )
//    return new_index
//  }
  def isActive( r: ma2.Range ) = (r != null) && (r != ma2.Range.VLEN)

  def section( ranges: List[ma2.Range] ): CDIndexMap = {
    assert(ranges.size == rank, "Bad ranges [] length")
    for( ii <-(0 until rank); r = ranges(ii); if( isActive(r) && (r != ma2.Range.EMPTY) )) {
      assert ((r.first >= 0) && (r.first < shape(ii)), "Bad range starting value at index " + ii + " => " + r.first + ", shape = " + shape(ii) )
      if( ( r.last < 0 ) || ( r.last >= shape(ii) ) )
        throw new Exception( "Bad range ending value at index " + ii + " => " + r.last + ", shape = " + shape(ii) )
    }
    var __offset: Int = _offset
    val _shape: Array[Int] = Array.fill[Int](rank)(0)
    val _stride: Array[Int] = Array.fill[Int](rank)(0)
    for( ii <-(0 until rank); r = ranges(ii) ) {
      if( !isActive(r) ) {
        _shape(ii) = shape(ii)
        _stride(ii) = stride(ii)
      }
      else if ( r == ma2.Range.EMPTY ) {
        _shape(ii) = 0
        _stride(ii) = 0
      } else {
        _shape(ii) = r.length
        _stride(ii) = stride(ii) * r.stride
        __offset += stride(ii) * r.first
      }
    }
    CDIndexMap( _shape, _stride, __offset )
  }

  def reduce: CDIndexMap = {
    val c: CDIndexMap = this
    for( ii <-(0 until rank); if (shape(ii) == 1) ) {
        val newc: CDIndexMap = c.reduce(ii)
        return newc.reduce
    }
    return c
  }

  def reduce(dim: Int): CDIndexMap = {
    assert((dim >= 0) && (dim < rank), "illegal reduce dim " + dim )
    assert( (shape(dim) == 1), "illegal reduce dim " + dim + " : length != 1" )
    val _shape = ListBuffer[Int]()
    val _stride = ListBuffer[Int]()
    for( ii <-(0 until rank); if (ii != dim) ) {
        _shape.append( shape(ii) )
        _stride.append( stride(ii) )
    }
    CDIndexMap( _shape.toArray, _stride.toArray, _offset )
  }

//  def transpose(index1: Int, index2: Int): CDIndexMap = {
//    assert((index1 >= 0) && (index1 < rank), "illegal index in transpose " + index1 )
//    assert((index2 >= 0) && (index2 < rank), "illegal index in transpose " + index1 )
//    val _shape = shape.clone()
//    val _stride = stride.clone()
//    _stride(index1) = stride(index2)
//    _stride(index2) = stride(index1)
//    _shape(index1) = shape(index2)
//    _shape(index2) = shape(index1)
//    CDIndexMap( _shape, _stride, _offset )
//  }
//
//  def permute(dims: Array[Int]): CDIndexMap = {
//    assert( (dims.length == shape.length), "illegal shape in permute " + dims )
//    for (dim <- dims) if ((dim < 0) || (dim >= rank)) throw new Exception( "illegal shape in permute " + dims )
//    val _shape = ListBuffer[Int]()
//    val _stride = ListBuffer[Int]()
//    for( i <-(0 until dims.length) ) {
//      _stride.append( stride(dims(i) ) )
//      _shape.append( shape(dims(i)) )
//    }
//    CDIndexMap( _shape.toArray, _stride.toArray, _offset )
//  }

  def broadcast( bcast_shape: Array[Int] ): CDIndexMap = {
    assert ( bcast_shape.length == rank, "Can't broadcast shape (%s) to (%s)".format( shape.mkString(","), bcast_shape.mkString(",") ) )
    val _shape = shape.clone()
    val _stride = stride.clone()
    for( idim <- (0 until rank ); bsize = bcast_shape(idim); size0 = shape(idim); if( bsize != size0 )  ) {
      assert((size0 == 1) || (bsize == size0) || _coordMaps.get(idim).isDefined, "Can't broadcast shape (%s) to (%s)".format(shape.mkString(","), bcast_shape.mkString(",")))
      _shape(idim) = bsize
      if( size0 == 1 ) { _stride(idim) = 0 }
    }
    CDIndexMap( _shape, _stride, _offset, _coordMaps )
  }
}

class CDCoordMap( val dimension: Int, val start: Int, val mapArray: Array[Int]  ) extends Serializable with Loggable {
  def this( dimension: Int, section: ma2.Section, mapArray: Array[Int] ) =  this( dimension, section.getRange(dimension).first(), mapArray )
  val nBins = mapArray.max + 1
  val length = mapArray.length

  def map( coordIndices: Array[Int] ): Array[Int] = {
    val result = coordIndices.clone()
    try {
      result(dimension) = mapArray(coordIndices(dimension))
      result
    } catch {
      case ex: java.lang.ArrayIndexOutOfBoundsException =>
        logger.error( " ArrayIndexOutOfBoundsException: mapArray[%d] = (%s), dimension=%d, coordIndices = (%s)".format( mapArray.size, mapArray.mkString(","), dimension, coordIndices.mkString(",") ) )
        throw ex
    }
  }
  override def toString = "CDCoordMap{ nbins=%d, dim=%d, _offset=%d, mapArray[%d]=[ %s ]}".format( nBins, dimension, start, mapArray.size, mapArray.mkString(", ") )

  def subset( section: ma2.Section ): CDCoordMap = {
    assert( start==0, "Attempt to subset a partitioned CoordMap: not supported.")
    val new_start: Int = section.getRange(dimension).first()
    logger.info( "CDCoordMap[%d].subset(%s): start=%d, until=%d, size=%d".format( dimension, section.toString, new_start, mapArray.length, mapArray.length-new_start) )
    new CDCoordMap( dimension, new_start, mapArray.slice( new_start, mapArray.length ) )
  }

  def ++( cmap: CDCoordMap ): CDCoordMap = {
    assert(dimension == cmap.dimension, "Attempt to combine incommensurate index maps, dimension mismatch: %d vs %d".format( dimension, cmap.dimension ) )
    if (dimension == 0) {
      assert(cmap.start == start + mapArray.length, "Attempt to combine incommensurate index maps, dimOffset mismatch: %d vs %d".format( cmap.start, start + mapArray.length )  )
      new CDCoordMap(dimension, start, mapArray ++ cmap.mapArray)
    } else {
      assert( mapArray == cmap.mapArray, "Attempt to combine incommensurate index maps" )
      clone.asInstanceOf[CDCoordMap]
    }
  }
}

class IndexValueAccumulator( start_value: Int = 0 ) {
  var current_index = Int.MaxValue
  var cum_index = start_value-1
  def getValue( index: Int ): Int = {
    if (index != current_index) { cum_index = cum_index + 1; current_index = index }
    cum_index
  }
}

class CDTimeCoordMap( val gridContext: GridContext, section: ma2.Section ) extends Loggable {
  val timeHelper = new ucar.nc2.dataset.CoordinateAxisTimeHelper( Calendar.gregorian, cdsutils.baseTimeUnits )
  val timeOffsets: Array[Long] = getTimeAxisData
  val time_axis_index = gridContext.getAxisIndex("t")

  def getDates(): Array[String] = { timeOffsets.map( timeHelper.makeCalendarDateFromOffset(_).toString ) }

  def getTimeAxisData(): Array[Long] = gridContext.getTimeAxisData match {
    case Some(array) =>  array.data
    case None => logger.error( "Cant get Time Axis Data" ); Array.emptyLongArray
  }

  def getTimeIndexIterator( resolution: String, range: ma2.Range ) = resolution match {
    case x if x.toLowerCase.startsWith("yea") => new YearOfCenturyIter( timeOffsets, range );
    case x if x.toLowerCase.startsWith("mon") => new MonthOfYearIter( timeOffsets, range );
    case x if x.toLowerCase.startsWith("day") => new DayOfYearIter( timeOffsets, range );
  }

  def pos_mod(initval: Int, period: Int): Int = if (initval >= 0) initval else pos_mod(initval + period, period)

  def getMontlyBinMap( section: ma2.Section ): CDCoordMap = {
    val timeIter = new MonthOfYearIter( timeOffsets,  section.getRange(0) );
    val accum = new IndexValueAccumulator()
    val timeIndices = for( time_index <- timeIter ) yield {time_index}
    new CDCoordMap( time_axis_index, section.getRange(time_axis_index).first(), timeIndices.toArray )
  }
  def getTimeCycleMap(period: Int, resolution: String, mod: Int, offset: Int, section: ma2.Section ): CDCoordMap = {
    val timeIter = getTimeIndexIterator( resolution, section.getRange(0) )
    val start_value = timeIter.getValue(0)-1
    val accum = new IndexValueAccumulator()
    assert(offset <= period, "TimeBin offset can't be >= the period.")
    val period_offest = pos_mod(offset - (start_value % period), period) % period
    val op_offset = (period - period_offest) % period
    val timeIndices = for (time_index <- timeIter; bin_index = accum.getValue(time_index)) yield {
      (( bin_index + op_offset ) / period) % mod
    }
    new CDCoordMap(time_axis_index, section.getRange(time_axis_index).first(), timeIndices.toArray )
  }
}


  //  def getTimeCycleMap( step: String, cycle: String, gridSpec: TargetGrid ): CDCoordMap = {
  //    val dimIndex: Int = gridSpec.getAxisIndex( "t" )
  //    val axisSpec  = gridSpec.grid.getAxisSpec( dimIndex )
  //    val units = axisSpec.coordAxis.getUnitsString
  //    axisSpec.coordAxis.getAxisType match {
  //      case AxisType.Time =>
  //        lazy val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(gridSpec.dataset.ncDataset, axisSpec.coordAxis, new Formatter())
  //        step match {
  //          case "month" =>
  //            if (cycle == "year") {
  //              new CDCoordMap( dimIndex, 12, timeAxis.getCalendarDates.map( _.getFieldValue(Month)-1 ).toArray )
  //            } else {
  //              val year_offset = timeAxis.getCalendarDate(0).getFieldValue(Year)
  //              val binIndices: Array[Int] =  timeAxis.getCalendarDates.map( cdate => cdate.getFieldValue(Month)-1 + cdate.getFieldValue(Year) - year_offset ).toArray
  //              new CDCoordMap( dimIndex, Math.ceil(axisSpec.coordAxis.getShape(0)/12.0).toInt, binIndices )
  //            }
  //          case "year" =>
  //            val year_offset = timeAxis.getCalendarDate(0).getFieldValue(Year)
  //            val binIndices: Array[Int] =  timeAxis.getCalendarDates.map( cdate => cdate.getFieldValue(Year) - year_offset ).toArray
  //            new CDCoordMap( dimIndex, Math.ceil(axisSpec.coordAxis.getShape(0)/12.0).toInt, binIndices )
  //          case x => throw new Exception("Binning not yet implemented for this step type: %s".format(step))
  //        }
  //      case x => throw new Exception("Binning not yet implemented for this axis type: %s".format(x.getClass.getName))
  //    }
  //  }
//    val units = axisSpec..getUnitsString
//    axisSpec.coordAxis.getAxisType match {
//      case AxisType.Time =>
//        lazy val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(gridSpec.dataset.ncDataset, axisSpec.coordAxis, new Formatter())
//        step match {
//          case "month" =>
//            if (cycle == "year") {
//              new CDCoordMap( dimIndex, 12, timeAxis.getCalendarDates.map( _.getFieldValue(Month)-1 ).toArray )
//            } else {
//              val year_offset = timeAxis.getCalendarDate(0).getFieldValue(Year)
//              val binIndices: Array[Int] =  timeAxis.getCalendarDates.map( cdate => cdate.getFieldValue(Month)-1 + cdate.getFieldValue(Year) - year_offset ).toArray
//              new CDCoordMap( dimIndex, Math.ceil(axisSpec.coordAxis.getShape(0)/12.0).toInt, binIndices )
//            }
//          case "year" =>
//            val year_offset = timeAxis.getCalendarDate(0).getFieldValue(Year)
//            val binIndices: Array[Int] =  timeAxis.getCalendarDates.map( cdate => cdate.getFieldValue(Year) - year_offset ).toArray
//            new CDCoordMap( dimIndex, Math.ceil(axisSpec.coordAxis.getShape(0)/12.0).toInt, binIndices )
//          case x => throw new Exception("Binning not yet implemented for this step type: %s".format(step))
//        }
//      case x => throw new Exception("Binning not yet implemented for this axis type: %s".format(x.getClass.getName))
//    }
//  }







