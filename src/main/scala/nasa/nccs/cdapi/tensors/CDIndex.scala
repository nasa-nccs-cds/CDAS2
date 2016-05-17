// Based on ucar.ma2.Index, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

package nasa.nccs.cdapi.tensors
import java.util.Formatter
import nasa.nccs.cdapi.cdm.CDSVariable
import scala.collection.mutable.ListBuffer
import ucar.ma2
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis1D, CoordinateAxis1DTime}
import ucar.nc2.time.CalendarPeriod.Field._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CDIndexMap {

  def factory(index: CDIndexMap): CDIndexMap = new CDIndexMap(index.getShape, index.getStride, index.getOffset )
  def factory(shape: Array[Int], stride: Array[Int]=Array.emptyIntArray, offset: Int = 0): CDIndexMap = new CDIndexMap(shape, stride, offset )
}

class CDIndexMap( protected val shape: Array[Int], _stride: Array[Int]=Array.emptyIntArray, protected val offset: Int = 0 ) {
  protected val rank: Int = shape.length
  protected val stride = if( _stride.isEmpty ) computeStrides(shape) else _stride
  def this( index: CDIndexMap ) = this( index.shape, index.stride, index.offset )

  def getRank: Int = rank
  def getShape: Array[Int] = shape.clone
  def getStride: Array[Int] = stride.clone
  def getShape(index: Int): Int = shape(index)
  def getSize: Int = shape.filter( _ > 0 ).product
  def getOffset: Int = offset
  def getReducedShape: Array[Int] = { ( for( idim <- ( 0 until rank) ) yield if( stride(idim) == 0 ) 1 else shape( idim ) ).toArray }
  override def toString: String = "{ Shape: " + shape.mkString("[ ",", "," ], Stride: " + stride.mkString("[ ",", "," ]") + " Offset: " + offset + " } ")

  def broadcasted: Boolean = {
    for( i <- (0 until rank) ) if( (stride(i) == 0) && (shape(i) > 1) ) return true
    false
  }

  def getCoordIndices( flatIndex: Int ): IndexedSeq[Int] = {
    var currElement = flatIndex
    currElement -= offset
    for( ii <-(0 until rank ) ) yield if (shape(ii) < 0) {  -1 } else {
      val coordIndex = currElement / stride(ii)
      currElement -= coordIndex * stride(ii)
      coordIndex
    }
  }

  def getStorageIndex( coordIndices: Array[Int] ): Int = {
    assert( coordIndices.length == rank, "Wrong number of coordinates in getStorageIndex for Array of rank %d: %d".format( rank, coordIndices.length) )
    var value: Int = offset
    for( ii <-(0 until rank ); if (shape(ii) >= 0) ) {
      value += coordIndices(ii) * stride(ii)
    }
    value
  }

  def computeStrides( shape: Array[Int] ): Array[Int] = {
    var product: Int = 1
    var strides = for (ii <- (shape.length - 1 to 0 by -1); thisDim = shape(ii) ) yield
      if (thisDim >= 0) {
        val curr_stride = product
        product *= thisDim
        curr_stride
      } else { 0 }
    return strides.reverse.toArray
  }

  def flip(index: Int): CDIndexMap = {
    assert ( (index >= 0) && (index < rank), "Illegal rank index: " +  index )
    val new_index = if (shape(index) >= 0) {
      val _offset = offset + stride(index) * (shape(index) - 1)
      val _stride = stride.clone
      _stride(index) = -stride(index)
      new CDIndexMap( shape, _stride, _offset )
    } else new CDIndexMap( this )
    return new_index
  }

  def section( ranges: List[ma2.Range] ): CDIndexMap = {
    assert(ranges.size == rank, "Bad ranges [] length")
    for( ii <-(0 until rank); r = ranges(ii); if ((r != null) && (r != ma2.Range.VLEN)) ) {
      assert ((r.first >= 0) && (r.first < shape(ii)), "Bad range starting value at index " + ii + " == " + r.first)
      assert ((r.last >= 0) && (r.last < shape(ii)), "Bad range ending value at index " + ii + " == " + r.last)
    }
    var _offset: Int = offset
    val _shape: Array[Int] = Array.fill[Int](rank)(0)
    val _stride: Array[Int] = Array.fill[Int](rank)(0)
    for( ii <-(0 until rank); r = ranges(ii) ) {
      if (r == null) {
        _shape(ii) = shape(ii)
        _stride(ii) = stride(ii)
      }
      else {
        _shape(ii) = r.length
        _stride(ii) = stride(ii) * r.stride
        _offset += stride(ii) * r.first
      }
    }
    CDIndexMap.factory( _shape, _stride, _offset )
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
    CDIndexMap.factory( _shape.toArray, _stride.toArray, offset )
  }

  def transpose(index1: Int, index2: Int): CDIndexMap = {
    assert((index1 >= 0) && (index1 < rank), "illegal index in transpose " + index1 )
    assert((index2 >= 0) && (index2 < rank), "illegal index in transpose " + index1 )
    val _shape = shape.clone()
    val _stride = stride.clone()
    _stride(index1) = stride(index2)
    _stride(index2) = stride(index1)
    _shape(index1) = shape(index2)
    _shape(index2) = shape(index1)
    CDIndexMap.factory( _shape, _stride, offset )
  }

  def permute(dims: Array[Int]): CDIndexMap = {
    assert( (dims.length == shape.length), "illegal shape in permute " + dims )
    for (dim <- dims) if ((dim < 0) || (dim >= rank)) throw new Exception( "illegal shape in permute " + dims )
    val _shape = ListBuffer[Int]()
    val _stride = ListBuffer[Int]()
    for( i <-(0 until dims.length) ) {
      _stride.append( stride(dims(i) ) )
      _shape.append( shape(dims(i)) )
    }
    CDIndexMap.factory( _shape.toArray, _stride.toArray, offset )
  }

  def broadcast(  dim: Int, size: Int ): CDIndexMap = {
    assert( shape(dim) == 1, "Can't broadcast a dimension with size > 1" )
    val _shape = shape.clone()
    val _stride = stride.clone()
    _shape(dim) = size
    _stride(dim) = 0
    CDIndexMap.factory( _shape, _stride, offset )
  }

  def broadcast( bcast_shape: Array[Int] ): CDIndexMap = {
    assert ( bcast_shape.length == rank, "Can't broadcast shape (%s) to (%s)".format( shape.mkString(","), bcast_shape.mkString(",") ) )
    val _shape = shape.clone()
    val _stride = stride.clone()
    for( idim <- (0 until rank ); bsize = bcast_shape(idim); size0 = shape(idim); if( bsize != size0 )  ) {
      assert((size0 == 1) || (bsize == size0), "Can't broadcast shape (%s) to (%s)".format(shape.mkString(","), bcast_shape.mkString(",")))
      _shape(idim) = bsize
      _stride(idim) = 0
    }
    CDIndexMap.factory( _shape, _stride, offset )
  }
}

trait CDCoordMapBase {
  def dimIndex: Int
  val nBins: Int
  def map( coordIndices: Array[Int] ): Array[Int]
  def mapShape( shape: Array[Int] ): Array[Int] = { val new_shape=shape.clone; new_shape(dimIndex)=nBins; new_shape }
}

class CDCoordMap( val dimIndex: Int, val nBins: Int, val mapArray: Array[Int] ) extends CDCoordMapBase {
  def map( coordIndices: Array[Int] ): Array[Int] = {
    val result = coordIndices.clone()
    result( dimIndex ) = mapArray( coordIndices(dimIndex) )
    result
  }
}

object CDTimeCoordMap {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def getTimeCycleMap( step: String, cycle: String, variable: CDSVariable ): CDCoordMap = {
    val dimIndex: Int = variable.getAxisIndex( 't' )
    val coordinateAxis: CoordinateAxis1D = variable.dataset.getCoordinateAxis( 't' ) match {
      case caxis: CoordinateAxis1D => caxis;
      case x => throw new Exception("Coordinate Axis type %s can't currently be binned".format(x.getClass.getName))
    }
    val units = coordinateAxis.getUnitsString
    coordinateAxis.getAxisType match {
      case AxisType.Time =>
        lazy val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(variable.dataset.ncDataset, coordinateAxis, new Formatter())
        step match {
          case "month" =>
            if (cycle == "year") {
              new CDCoordMap( dimIndex, 12, timeAxis.getCalendarDates.map( _.getFieldValue(Month)-1 ).toArray )
            } else {
              val year_offset = timeAxis.getCalendarDate(0).getFieldValue(Year)
              val binIndices: Array[Int] =  timeAxis.getCalendarDates.map( cdate => cdate.getFieldValue(Month)-1 + cdate.getFieldValue(Year) - year_offset ).toArray
              new CDCoordMap( dimIndex, Math.ceil(coordinateAxis.getShape(0)/12.0).toInt, binIndices )
            }
          case "year" =>
            val year_offset = timeAxis.getCalendarDate(0).getFieldValue(Year)
            val binIndices: Array[Int] =  timeAxis.getCalendarDates.map( cdate => cdate.getFieldValue(Year) - year_offset ).toArray
            new CDCoordMap( dimIndex, Math.ceil(coordinateAxis.getShape(0)/12.0).toInt, binIndices )
          case x => throw new Exception("Binning not yet implemented for this step type: %s".format(step))
        }
      case x => throw new Exception("Binning not yet implemented for this axis type: %s".format(x.getClass.getName))
    }
  }
}











