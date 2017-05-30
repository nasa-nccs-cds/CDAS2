package nasa.nccs.utilities

import java.lang.management.ManagementFactory

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

object TimeStamp {
  val pid = ManagementFactory.getRuntimeMXBean.getName
  val eid = SparkEnv.get.executorId
  def apply( startTime: Long, label: String ): TimeStamp = { new TimeStamp( (System.currentTimeMillis()-startTime)/1.0E3f, label ) }
}

class TimeStamp( val elapasedJobTime: Float, val label: String ) extends Serializable with Ordered [TimeStamp]  {
  import TimeStamp._
  val tid = s"TimeStamp[${eid}:${pid}]"
  override def toString(): String = { s"TimeStamp[${tid}] { ${elapasedJobTime.toString} => $label" }
  def compare (that: TimeStamp) = { elapasedJobTime.compareTo( that.elapasedJobTime ) }
}

object ProfilingTool extends Loggable {

  implicit def listAccum[TimeStamp]: AccumulableParam[mutable.ListBuffer[TimeStamp], TimeStamp] =
    new AccumulableParam[mutable.ListBuffer[TimeStamp], TimeStamp] {
      def addInPlace(t1: mutable.ListBuffer[TimeStamp], t2: mutable.ListBuffer[TimeStamp]) : mutable.ListBuffer[TimeStamp] = { t1 ++= t2; t1 }
      def addAccumulator(t1: mutable.ListBuffer[TimeStamp], t2: TimeStamp) : mutable.ListBuffer[TimeStamp] = { t1 += t2; t1 }
      def zero(t: mutable.ListBuffer[TimeStamp]) : mutable.ListBuffer[TimeStamp] = { new mutable.ListBuffer[TimeStamp]() }
    }

  def apply( sparkContext: SparkContext ): ProfilingTool = {
    val startTimeMS: Long = System.currentTimeMillis()
    val starting_timestamp = new TimeStamp( 0f, "Job Start")
    val timestamps: Accumulable[mutable.ListBuffer[TimeStamp], TimeStamp] = sparkContext.accumulable(new mutable.ListBuffer[TimeStamp]())
    val profiler = new ProfilingTool( startTimeMS, timestamps )
    logger.info( s"Starting profiler in sparkContext '${sparkContext.applicationId}' with master '${sparkContext.master}' ")
    profiler.timestamp("Startup")
    profiler
  }
}

class ProfilingTool( val startTime: Long, timestamps: Accumulable[mutable.ListBuffer[TimeStamp], TimeStamp] ) extends Serializable {
  def timestamp( label: String ): Unit = { timestamps += TimeStamp( startTime, label ) }
  def getTimestamps: List[TimeStamp] = timestamps.value.sorted.toList
  override def toString = " *** TIMESTAMPS: ***\n\n\t" + getTimestamps.map( _.toString() ).mkString("\n\t") + "\n\n"
}
