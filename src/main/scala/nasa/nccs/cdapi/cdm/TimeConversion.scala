package nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.data.HeapFltArray
import nasa.nccs.cdapi.tensors.CDFloatArray
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.ma2

class RemapElem( val index: Int, val weight0: Float, val weight1: Float) {

}

class TimeAxisConverter( val toAxis: CoordinateAxis1DTime, val fromAxis: CoordinateAxis1DTime, val toAxisRange: ma2.Range = ma2.Range.EMPTY ) {
  protected val range = if( toAxisRange == ma2.Range.EMPTY ) { new ma2.Range(toAxis.getSize.toInt) } else toAxisRange
  protected val weights: Map[Int,RemapElem] = computeWeights()

  def computeWeights(): Map[Int,RemapElem] = {
    val buf = scala.collection.mutable.ListBuffer.empty[(Int,RemapElem)]
    val iter  = range.getIterator
    while ( iter.hasNext ) {
      val index = iter.next
      val cdate0 = toAxis.getCalendarDate(index)
      val fromIndex = fromAxis.findTimeIndexFromCalendarDate(cdate0)
      val (cd0,cd1) = (fromAxis.getCalendarDate(fromIndex),fromAxis.getCalendarDate(fromIndex+1))
      val (dt0,dt1,dt) = ( cd0.getDifferenceInMsecs(cdate0), cdate0.getDifferenceInMsecs(cd1), cd0.getDifferenceInMsecs(cd1) )
      buf += ( index -> new RemapElem( fromIndex, dt1/dt, dt0/dt ) )
    }
    Map( buf: _* )
  }

//  def convert( data: CDFloatArray ): CDFloatArray = {
//    data.slice(0,)
//  }

}
