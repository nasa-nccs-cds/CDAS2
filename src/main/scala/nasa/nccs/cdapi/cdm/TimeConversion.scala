package nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.data.HeapFltArray
import nasa.nccs.cdapi.tensors.CDFloatArray
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.ma2

class RemapElem( val index: Int, val weight0: Float, val weight1: Float) extends Serializable  { }

object TimeConversionSpec {
  def apply ( weights: Map[Int,RemapElem], toAxisRange: ma2.Range ): TimeConversionSpec = {
    new TimeConversionSpec( weights, ( toAxisRange.first,  toAxisRange.last ) )
  }
}
class TimeConversionSpec( val weights: Map[Int,RemapElem], val toAxisRange: (Int,Int) ) extends Serializable {
  def mapOrigin( old_origin: Array[Int] ): Array[Int] = {
    old_origin.zipWithIndex.map{ case (o,i) => if( i == 0 ) ( toAxisRange._1 ) else o }
  }
  def toSize = toAxisRange._2 - toAxisRange._1 + 1
}

object TimeAxisConverter {
  def apply ( toAxis: CoordinateAxis1DTime, fromAxis: CoordinateAxis1DTime, toAxisRange: ma2.Range ): TimeAxisConverter = {
    new TimeAxisConverter( toAxis, fromAxis, ( toAxisRange.first,  toAxisRange.last ) )
  }
}

class TimeAxisConverter( val toAxis: CoordinateAxis1DTime, val fromAxis: CoordinateAxis1DTime, val toAxisRange: (Int,Int) ) {

  def computeWeights(): TimeConversionSpec = {
    val buf = scala.collection.mutable.ListBuffer.empty[(Int,RemapElem)]
    for ( index <- (toAxisRange._1 to toAxisRange._2) ) {
      val cdate0 = toAxis.getCalendarDate(index)
      val fromIndex = fromAxis.findTimeIndexFromCalendarDate(cdate0)
      val toIndex = Math.min( fromIndex+1, fromAxis.getSize -1 ).toInt
      val (cd0,cd1) = (fromAxis.getCalendarDate(fromIndex),fromAxis.getCalendarDate( toIndex ) )
      val dt = { val dt0 = cd0.getDifferenceInMsecs(cd1); if( dt0 == 0.0 ) 1.0 else dt0; }
      val (w0,w1) = ( cd0.getDifferenceInMsecs(cdate0)/dt, cdate0.getDifferenceInMsecs(cd1)/dt )
      buf += ( index -> new RemapElem( fromIndex, w0.toFloat, w1.toFloat ) )
    }
    new TimeConversionSpec( Map( buf: _* ), toAxisRange )
  }

}