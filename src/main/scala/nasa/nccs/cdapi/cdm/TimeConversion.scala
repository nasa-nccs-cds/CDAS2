package nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.data.HeapFltArray
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdas.engine.spark.{RecordKey, RecordKey$}
import nasa.nccs.utilities.Loggable
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.ma2

class RemapElem( val index: Int, val weight0: Float, val weight1: Float) extends Serializable  { }

object TimeConversionSpec {
  def apply ( weights: Map[Int,RemapElem], toAxisRange: ma2.Range, toCoordRange: (Long,Long) ): TimeConversionSpec = {
    new TimeConversionSpec( weights, ( toAxisRange.first,  toAxisRange.last ), toCoordRange )
  }
}
class TimeConversionSpec( val weights: Map[Int,RemapElem], val toAxisRange: (Int,Int), val toCoordRange: (Long,Long) ) extends Serializable {
  def mapOrigin( old_origin: Array[Int] ): Array[Int] = {
    old_origin.zipWithIndex.map{ case (o,i) => if( i == 0 ) toAxisRange._1 else o }
  }
  def toSize = toAxisRange._2 - toAxisRange._1 + 1
  def getPartKey: RecordKey = RecordKey( toCoordRange._1, toCoordRange._2, toAxisRange._1, toAxisRange._2-toAxisRange._1 )
}

object TimeAxisConverter {
  def apply ( toAxis: CoordinateAxis1DTime, fromAxis: CoordinateAxis1DTime, toAxisRange: ma2.Range ): TimeAxisConverter = {
    new TimeAxisConverter( toAxis, fromAxis, ( toAxisRange.first,  toAxisRange.last ) )
  }
}

class TimeAxisConverter( val toAxis: CoordinateAxis1DTime, val fromAxis: CoordinateAxis1DTime, val toAxisRange: (Int,Int) ) extends Loggable {

  def computeWeights(): TimeConversionSpec = {
    val buf = scala.collection.mutable.ListBuffer.empty[(Int,RemapElem)]
    for ( index <- (toAxisRange._1 to toAxisRange._2) ) {
      val cdate0 = toAxis.getCalendarDate(index)
      val fromIndex = fromAxis.findTimeIndexFromCalendarDate(cdate0)
      val toIndex = Math.min( fromIndex+1, fromAxis.getSize -1 ).toInt
      val (cd0,cd1) = (fromAxis.getCalendarDate(fromIndex),fromAxis.getCalendarDate( toIndex ) )
      val dt = { val dt0 = cd0.getDifferenceInMsecs(cd1); if( dt0 == 0.0 ) 1.0 else dt0; }
      val (w0,w1) = ( cdate0.getDifferenceInMsecs(cd1)/dt, cd0.getDifferenceInMsecs(cdate0)/dt )
      buf += ( index -> new RemapElem( fromIndex, w0.toFloat, w1.toFloat ) )
      logger.debug( s"  TimeAxisConverter ==> Weight[$index]{${cdate0.toString}} from W($w0):I[$fromIndex]:T{${cd0.toString}} - W($w1):I[$toIndex]:T{${cd1.toString}}")
    }
    val toCoordRange = ( toAxis.getCalendarDate(toAxisRange._1).getMillis/1000, toAxis.getCalendarDate(toAxisRange._2).getMillis/1000 )
    new TimeConversionSpec( Map( buf: _* ), toAxisRange, toCoordRange )
  }

}
