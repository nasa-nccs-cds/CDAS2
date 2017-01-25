package nasa.nccs.cdapi.cdm
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.ma2

class TimeRegridder( val fromAxis: CoordinateAxis1DTime, val toAxis: CoordinateAxis1DTime, val range: ma2.Range = ma2.Range.EMPTY ) {
  val weights = computeWeights()

  def computeWeights() = {
    val actualRange = if( range == ma2.Range.EMPTY ) { Range(0,fromAxis.getSize.toInt-1) } else range

  }

}
