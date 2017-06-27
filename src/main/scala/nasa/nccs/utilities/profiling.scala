package nasa.nccs.utilities

import java.lang.management.ManagementFactory

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

object TimeStamp {
  def apply( startTime: Long, label: String ): TimeStamp = { new TimeStamp( (System.currentTimeMillis()-startTime)/1.0E3f, label ) }

  def getWorkerSignature: String = {
    val thread: Thread = Thread.currentThread()
    val node_name = ManagementFactory.getRuntimeMXBean.getName.split("[@]").last.split("[.]").head
    val worker_name = thread.getName.split("[-]").last
    s"${node_name}-E${SparkEnv.get.executorId}-W${worker_name}"
  }
}

class TimeStamp( val elapasedJobTime: Float, val label: String ) extends Serializable with Ordered [TimeStamp] with Loggable {
  import TimeStamp._
  val tid = s"TimeStamp[${getWorkerSignature}]"
  val sval = s"TIME[${getWorkerSignature}] { ${elapasedJobTime.toString} => $label }"
  override def toString(): String = { sval }
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

class ProfilingTool( val startTime: Long, timestamps: Accumulable[mutable.ListBuffer[TimeStamp], TimeStamp] ) extends Serializable with Loggable {
  def timestamp( label: String, log: Boolean = false ): Unit = {
    timestamps += TimeStamp( startTime, label )
    if( log ) { logger.info(label) }
  }
  def getTimestamps: List[TimeStamp] = timestamps.value.sorted.toList
  override def toString = " *** TIMESTAMPS: ***\n\n\t" + getTimestamps.map( _.toString() ).mkString("\n\t") + "\n\n"
}
