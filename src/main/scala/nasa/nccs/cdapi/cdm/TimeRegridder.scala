package nasa.nccs.cdapi.cdm
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.ma2

class TimeRegridder( val toAxis: CoordinateAxis1DTime, val fromAxis: CoordinateAxis1DTime, val toAxisRange: ma2.Range = ma2.Range.EMPTY ) {
  val weights = computeWeights()

  def computeWeights() = {
    val actualRange = if( toAxisRange == ma2.Range.EMPTY ) { new ma2.Range(toAxis.getSize.toInt) } else toAxisRange
    val iter  = actualRange.getIterator
    while ( iter.hasNext ) {
      val index = iter.next
      val cdate0 = toAxis.getCalendarDate(index)
      val fromIndex = fromAxis.findTimeIndexFromCalendarDate(cdate0)
    }

  }

}
