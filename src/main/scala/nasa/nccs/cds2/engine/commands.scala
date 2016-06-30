package nasa.nccs.cds2.engine
import nasa.nccs.esgf.process.TaskRequest
import java.io.{IOException, PrintWriter, StringWriter}
import java.nio.FloatBuffer

import nasa.nccs.cdapi.cdm.{Collection, PartitionedFragment, _}
import nasa.nccs.cds2.loaders.{Collections, Masks}
import nasa.nccs.esgf.process._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import nasa.nccs.utilities.{Loggable, cdsutils}
import nasa.nccs.cds2.kernels.KernelMgr
import nasa.nccs.cdapi.kernels._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}
import java.util.concurrent.atomic.AtomicReference

import nasa.nccs.cdapi.tensors.{CDArray, CDByteArray, CDFloatArray}
import nasa.nccs.caching._
import ucar.{ma2, nc2}
import nasa.nccs.cds2.utilities.GeoTools

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.concurrent._

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap

object TaskExecutorSync extends Loggable {
  val printer = new scala.xml.PrettyPrinter(200, 3)
  val executionManager = new CDS2ExecutionManager(Map.empty)
  private val taskMap = new ConcurrentLinkedHashMap.Builder[String,TaskRequest].initialCapacity(100).maximumWeightedCapacity(10000).build()
  private val executionCache: Cache[String,ExecutionResults] = new FutureCache("Store","execution",false)
  private var taskIndex = 0

  def execute(tr: TaskRequest, run_args: Map[String,String] = Map.empty[String,String] ) {
    val tid = getTaskId
    taskMap.put( tid, tr )
    logger.info( s" >>>> Executing task '$tid'" )
    val fragFuture = executionCache( tid ) { promiseExecutionResults(tr,run_args) _ }
    fragFuture onComplete {
      case Success(result) =>  logger.info( s" >>>> Final Result for task '$tid': " + printer.format(result.toXml) )
      case Failure(err) => logger.warn( s" Failed to execute task '$tid' due to error: " + err.getMessage )
    }
  }
  def getTaskId: String = { taskIndex += 1; "t" + taskIndex }
  def promiseExecutionResults(tr: TaskRequest, run_args: Map[String,String])(p: Promise[ExecutionResults]) = {
    val response = executionManager.blockingExecute (tr, run_args ++ Map("async" -> "false") )
    response.results.head match {
      case result: BlockingExecutionResult => p.success( response )
      case xmlresult: XmlExecutionResult => p.success( response )
      case err: ErrorExecutionResult => p.failure( err.err )
      case x => p.failure ( new Exception( "Unexpected response, got " + x.toString ) )
    }
  }
}

object MetadataPrinter {
  def display( level: Int ) = {
    val dataInputs: Map[String, Seq[Map[String, Any]]] = level match {
      case 0 => Map()
      case 1 => Map("variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0")))
    }
    val tr = TaskRequest("CDS.metadata", dataInputs)
    TaskExecutorSync.execute(tr)
  }
}




