package nasa.nccs.cdapi.cdm
import ucar.ma2

class SectionPartitioner ( val roi: ma2.Section, val nPartitions: Int ) {


  def getPartition( part_index: Int, axis_index: Int ): Option[ma2.Section] = {
    if( part_index >= nPartitions ) return None
    val partSection = new ma2.Section(roi)
    val range = roi.getRange(axis_index)
    if( part_index >= range.length ) return None
    val step = range.length / nPartitions
    val excess = range.length - step*nPartitions
    val first = if(part_index<excess) part_index*(step+1) else excess*(step+1) + (part_index-excess)*step
    val last = if(part_index<excess) first + step else first + step - 1
    Some( partSection.replaceRange( axis_index, new ma2.Range( first, last) ) )
  }

}

object partitionTest extends App {
  val roi = new ma2.Section()
  val r0 = new ma2.Range( 0, 211, 1)
  roi.appendRange( r0 )
  val nPart = 22
  var total_ranges = 0
  val sp = new SectionPartitioner( roi, nPart )
  for( i <- 0 to nPart ) {
      sp.getPartition(i,0) match {
      case Some(part1) =>
        println( part1.toString )
        total_ranges += part1.getRange(0).length()
      case x => x
    }
  }
  println( "%d == %d ?".format( r0.length, total_ranges ) )
}

