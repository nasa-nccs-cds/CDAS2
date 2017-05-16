package nasa.nccs.utilities

import org.apache.spark.{Accumulable, AccumulableParam, Accumulator, SparkContext}
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable

object TimeStamp {
  def apply( startTime: Long, label: String ): TimeStamp = { new TimeStamp( (System.currentTimeMillis()-startTime)/1.0E6f, label ) }
}

class TimeStamp( val elapasedJobTime: Float, val label: String ) extends Serializable with Ordered [TimeStamp]  {
  override def toString(): String = { s"TimeStamp { ${elapasedJobTime.toString} => $label" }
  def compare (that: TimeStamp) = { elapasedJobTime.compareTo( that.elapasedJobTime ) }
}

object ProfilingTool {

  implicit def listAccum[TimeStamp]: AccumulableParam[mutable.ListBuffer[TimeStamp], TimeStamp] =
    new AccumulableParam[mutable.ListBuffer[TimeStamp], TimeStamp] {
      def addInPlace(t1: mutable.ListBuffer[TimeStamp], t2: mutable.ListBuffer[TimeStamp]) : mutable.ListBuffer[TimeStamp] = { t1 ++= t2; t1 }
      def addAccumulator(t1: mutable.ListBuffer[TimeStamp], t2: TimeStamp) : mutable.ListBuffer[TimeStamp] = { t1 += t2; t1 }
      def zero(t: mutable.ListBuffer[TimeStamp]) : mutable.ListBuffer[TimeStamp] = { new mutable.ListBuffer[TimeStamp]() }
    }

  def apply( sparkContext: SparkContext ): ProfilingTool = {
    val startTimeMS: Long = System.currentTimeMillis()
    val broadcastedStartTime: Broadcast[Long] = sparkContext.broadcast( startTimeMS )
    val starting_timestamp = new TimeStamp( 0f, "Job Start")
    val timestamps: Accumulable[mutable.ListBuffer[TimeStamp], TimeStamp] = sparkContext.accumulable(new mutable.ListBuffer[TimeStamp]())
    new ProfilingTool( broadcastedStartTime, timestamps )
  }
}

class ProfilingTool( val startTime: Broadcast[Long], timestamps: Accumulable[mutable.ListBuffer[TimeStamp], TimeStamp] ) extends Serializable {
  def timestamp( label: String ): Unit = { timestamps += TimeStamp( startTime.value, label ) }
  def getTimestamps: List[TimeStamp] = timestamps.value.sorted.toList
  override def toString = "\n\n\t\t\tTIMESTAMPS:\n\t" + getTimestamps.map( _.toString() ).mkString("\n\t") + "\n\n"
}
