package nasa.nccs.cds2.engine
import java.io.{IOException, PrintWriter, StringWriter}
import java.nio.FloatBuffer

import nasa.nccs.cdapi.cdm.{Collection, PartitionedFragment, _}
import nasa.nccs.cds2.loaders.{Collections, Masks}
import nasa.nccs.esgf.process._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import nasa.nccs.utilities.cdsutils
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


class Counter(start: Int = 0) {
  private val index = new AtomicReference(start)
  def get: Int = {
    val i0 = index.get
    if(index.compareAndSet( i0, i0 + 1 )) i0 else get
  }
}

class MetadataOnlyException extends Exception {}

class CDS2ExecutionManager( val serverConfiguration: Map[String,String] ) {
  val serverContext = new ServerContext( collectionDataCache, serverConfiguration )
  val logger = LoggerFactory.getLogger(this.getClass)
  val kernelManager = new KernelMgr()
  private val counter = new Counter

  def getKernelModule( moduleName: String  ): KernelModule = {
    kernelManager.getModule( moduleName ) match {
      case Some(kmod) => kmod
      case None => throw new Exception("Unrecognized Kernel Module %s, modules = %s ".format( moduleName, kernelManager.getModuleNames.mkString("[ ",", "," ]") ) )
    }
  }
  def getResourcePath( resource: String ): Option[String] = Option(getClass.getResource(resource)).map( _.getPath )

  def getKernel( moduleName: String, operation: String  ): Kernel = {
    val kmod = getKernelModule( moduleName )
    kmod.getKernel( operation  ) match {
      case Some(kernel) => kernel
      case None => throw new Exception( s"Unrecognized Kernel %s in Module %s, kernels = %s ".format( operation, moduleName, kmod.getKernelNames.mkString("[ ",", "," ]")) )
    }
  }
  def getKernel( kernelName: String  ): Kernel = {
    val toks = kernelName.split('.')
    getKernel( toks.dropRight(1).mkString("."), toks.last )
  }

  def fatal(err: Throwable): String = {
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    err.getMessage
  }

  def createTargetGrid( request: TaskRequest ): TargetGrid = {
    request.targetGridSpec.get("id") match {
      case Some(varId) => request.variableMap.get(varId) match {
        case Some(dataContainer: DataContainer) => serverContext.createTargetGrid( dataContainer, request.getDomain(dataContainer.getSource) )
        case None => varId match {
          case "#META" => throw new MetadataOnlyException()
          case x => throw new Exception("Unrecognized variable id in Grid spec: " + varId)
        }
      }
      case None => throw new Exception("Target grid specification method has not yet been implemented: " + request.targetGridSpec.toString)
    }
  }

  def loadInputData( request: TaskRequest, targetGrid: TargetGrid, run_args: Map[String,String] ): RequestContext = {
    val t0 = System.nanoTime
    val sourceContainers = request.variableMap.values.filter(_.isSource)
    val t1 = System.nanoTime
    val sources = for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource; domainOpt = request.getDomain(data_container.getSource) )
      yield serverContext.createInputSpec(data_container, domainOpt, targetGrid, request.getDataAccessMode )
    val t2 = System.nanoTime
    val sourceMap: Map[String,OperationInputSpec] = Map(sources.toSeq:_*)
    val rv = new RequestContext (request.domainMap, sourceMap, targetGrid, run_args)
    val t3 = System.nanoTime
    logger.info( " LoadInputDataT: %.4f %.4f %.4f".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9, (t3-t2)/1.0E9 ) )
    rv
  }

  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[ExecutionResults] = Future {
    val targetGrid: TargetGrid = createTargetGrid( request )
    val requestContext = loadInputData( request, targetGrid, run_args )
    executeWorkflows( request, requestContext )
  }

  def getRequestContext( request: TaskRequest, run_args: Map[String,String] ): RequestContext = loadInputData( request, createTargetGrid( request ), run_args )

  def blockingExecute( request: TaskRequest, run_args: Map[String,String] ): ExecutionResults =  {
    logger.info("Blocking Execute { runargs: " + run_args.toString + ",  request: " + request.toString + " }")
    val t0 = System.nanoTime
    try {
      val targetGrid: TargetGrid = createTargetGrid( request )
      val t1 = System.nanoTime
      val requestContext = loadInputData( request, targetGrid, run_args )
      val t2 = System.nanoTime
      val rv = executeWorkflows( request, requestContext )
      val t3 = System.nanoTime
      logger.info( "Execute Completed: CreateTargetGrid> %.4f, LoadVariablesT> %.4f, ExecuteWorkflowT> %.4f, totalT> %.4f ".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9, (t3-t2)/1.0E9, (t3-t0)/1.0E9 ) )
      rv
    } catch {
      case err: MetadataOnlyException => executeMetadataWorkflows( request )
      case err: Exception => new ExecutionResults(err)
    }
  }

//  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[xml.Elem] = Future {
//    try {
//      val sourceContainers = request.variableMap.values.filter(_.isSource)
//      val inputFutures: Iterable[Future[OperationInputSpec]] = for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource) yield {
//        serverContext.dataLoader.loadVariableDataFuture(data_container, request.getDomain(data_container.getSource))
//      }
//      inputFutures.flatMap( inputFuture => for( input <- inputFuture ) yield executeWorkflows(request, run_args).toXml )
//    } catch {
//      case err: Exception => fatal(err)
//    }
//  }

  def getResultFilePath( resultId: String ): Option[String] = {
    import java.io.File
    val resultFile = Kernel.getResultFile( serverConfiguration, resultId )
    if(resultFile.exists) Some(resultFile.getAbsolutePath) else None
  }

  def executeAsync( request: TaskRequest, run_args: Map[String,String] ): ( String, Future[ExecutionResults] ) = {
    logger.info("Execute { runargs: " + run_args.toString + ",  request: " + request.toString + " }")
    val async = run_args.getOrElse("async", "false").toBoolean
    val resultId = "r" + counter.get.toString
    val futureResult = this.futureExecute( request, Map( "resultId" -> resultId ) ++ run_args )
    futureResult onSuccess { case results: ExecutionResults =>
      println("Process Completed: " + results.toString )
      processAsyncResult( resultId, results )
    }
    futureResult onFailure { case e: Throwable => fatal( e ); throw e }
    (resultId, futureResult)
  }

  def processAsyncResult( resultId: String, results: ExecutionResults ) = {

  }

//  def execute( request: TaskRequest, runargs: Map[String,String] ): xml.Elem = {
//    val async = runargs.getOrElse("async","false").toBoolean
//    if(async) executeAsync( request, runargs ) else  blockingExecute( request, runargs )
//  }

  def describeProcess( kernelName: String ): xml.Elem = getKernel( kernelName ).toXml

  def listProcesses(): xml.Elem = kernelManager.toXml

  def executeWorkflows( request: TaskRequest, requestCx: RequestContext ): ExecutionResults = {
    new ExecutionResults( request.workflows.flatMap(workflow => workflow.operations.map( operationExecution( _, requestCx ))) )
  }
  def executeMetadataWorkflows( request: TaskRequest ): ExecutionResults = {
    new ExecutionResults( request.workflows.flatMap(workflow => workflow.operations.map( metadataExecution )) )
  }

  def executeUtility( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
    val result: xml.Node =  <result> {"Completed executing utility " + operationCx.name.toLowerCase } </result>
    new XmlExecutionResult( operationCx.name.toLowerCase + "~u0", result )
  }

  def operationExecution( operationCx: OperationContext, requestCx: RequestContext ): ExecutionResult = {
    val opName = operationCx.name.toLowerCase
    val module_name = opName.split('.')(0)
    module_name match {
      case "util" => executeUtility( operationCx, requestCx, serverContext )
      case x => getKernel( opName ).execute( operationCx, requestCx, serverContext )
    }
  }

  def metadataExecution( operationCx: OperationContext ): ExecutionResult = {
    val opName = operationCx.name.toLowerCase
    val module_name = opName.split('.')(0)
    getKernel(opName).execute( operationCx, serverContext )
  }
}

object SampleTaskRequests {

  def createTestData() = {
    var axes = Array("time","lev","lat","lon")
    var shape = Array(1,1,180,360)
    val maskedTensor: CDFloatArray = CDFloatArray( shape, Array.fill[Float](180*360)(1f), Float.MaxValue)
    val varname = "ta"
    val resultFile = "/tmp/SyntheticTestData.nc"
    val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, resultFile )
    val dims: IndexedSeq[nc2.Dimension] = shape.indices.map( idim => writer.addDimension(null, axes(idim), maskedTensor.getShape(idim)))
    val variable: nc2.Variable = writer.addVariable(null, varname, ma2.DataType.FLOAT, dims.toList)
    variable.addAttribute( new nc2.Attribute( "missing_value", maskedTensor.getInvalid ) )
    writer.create()
    writer.write( variable, maskedTensor )
    writer.close()
    println( "Writing result to file '%s'".format(resultFile) )
  }

  def getSpatialAve(collection: String, varname: String, weighting: String, level_index: Int = 0, time_index: Int = 0): TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> time_index, "end" -> time_index, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> s"collection:/$collection", "name" -> s"$varname:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"xy", "weights"->weighting ) ))
    TaskRequest( "CDS.average", dataInputs )
  }

  def getMaskedSpatialAve(collection: String, varname: String, weighting: String, level_index: Int = 0, time_index: Int = 0): TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "mask" -> "#ocean50m", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> time_index, "end" -> time_index, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> s"collection:/$collection", "name" -> s"$varname:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"xy", "weights"->weighting ) ))
    TaskRequest( "CDS.average", dataInputs )
  }

  def getConstant(collection: String, varname: String, level_index: Int = 0 ): TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> 10, "end" -> 10, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> s"collection:/$collection", "name" -> s"$varname:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0") ))
    TaskRequest( "CDS.const", dataInputs )
  }

  def getAnomalyTest: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List(Map("name" -> "d0", "lat" -> Map("start" -> -7.0854263, "end" -> -7.0854263, "system" -> "values"), "lon" -> Map("start" -> 12.075, "end" -> 12.075, "system" -> "values"), "lev" -> Map("start" -> 1000, "end" -> 1000, "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggtest", "name" -> "t:v0", "domain" -> "d0")),  // collection://merra300/hourly/asm_Cp
      "operation" -> List( Map( "input"->"v0", "axes"->"t" ) ))
    TaskRequest( "CDS.anomaly", dataInputs )
  }
}

object executionTest extends App {
  val request = SampleTaskRequests.getAnomalyTest
  val async = false
  val run_args = Map( "async" -> async.toString )
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val t0 = System.nanoTime
  if(async) {
    cds2ExecutionManager.executeAsync(request, run_args) match {
      case ( resultId: String, futureResult: Future[ExecutionResults] ) =>
        val t1 = System.nanoTime
        println ("Initial Result, time = %.4f ".format ((t1 - t0) / 1.0E9) )
        val result = Await.result (futureResult, Duration.Inf)
        val t2 = System.nanoTime
        println ("Final Result, time = %.4f, result = %s ".format ((t2 - t1) / 1.0E9, result.toString) )
      case x => println( "Unrecognized result from executeAsync: " + x.toString )
    }
   }
  else {
    val t1 = System.nanoTime
    val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
    val t2 = System.nanoTime
    println("Final Result, time = %.4f (%.4f): %s ".format( (t2-t1)/1.0E9, (t2-t0)/1.0E9, final_result.toString) )
  }
}

abstract class SyncExecutor {
  val printer = new scala.xml.PrettyPrinter(200, 3)

  def main(args: Array[String]) {
    val executionManager = getExecutionManager
    val final_result = getExecutionManager.blockingExecute( getTaskRequest, getRunArgs )
    println(">>>> Final Result: " + printer.format(final_result.toXml))
    FragmentPersistence.close()
  }

  def getTaskRequest(): TaskRequest
  def getRunArgs = Map("async" -> "false")
  def getExecutionManager = new CDS2ExecutionManager(Map.empty)
  def getCollection( id: String ): Collection = Collections.findCollection(id) match { case Some(collection) => collection; case None=> throw new Exception(s"Unknown Collection: $id" ) }
}

object TimeAveSliceTask extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "hur:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
    TaskRequest("CDS.average", dataInputs)
  }
}

object YearlyCycleSliceTask extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "period" -> 1, "unit" -> "month", "mod" -> 12)))
    TaskRequest("CDS.bin", dataInputs)
  }
}

object AveTimeseries extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer](new WorkflowContainer(operations = List(new OperationContext(identifier = "CDS.average~ivar#1", name = "CDS.average", rid = "ivar#1", inputs = List("v0"), Map("axis" -> "t")))))
    val variableMap = Map[String, DataContainer]("v0" -> new DataContainer(uid = "v0", source = Some(new DataSource(name = "hur", collection = getCollection("merra/mon/atmos"), domain = "d0"))))
    val domainMap = Map[String, DomainContainer]("d0" -> new DomainContainer(name = "d0", axes = cdsutils.flatlist(DomainAxis(Z, 1, 1), DomainAxis(Y, 100, 100), DomainAxis(X, 100, 100)), None))
    new TaskRequest("CDS.average", variableMap, domainMap, workflows, Map("id" -> "v0"))
  }
}

object CreateVTask extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
        Map("name" -> "d1", "time" -> Map("start" -> "2010-01-16T12:00:00", "end" -> "2010-01-16T12:00:00", "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "t", "name" -> "CDS.anomaly"), Map("input" -> "v0", "period" -> 1, "unit" -> "month", "mod" -> 12, "name" -> "CDS.timeBin"), Map("input" -> "v0", "domain" -> "d1", "name" -> "CDS.subset")))
    TaskRequest("CDS.workflow", dataInputs)
  }
}

object YearlyCycleTask extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "period" -> 1, "unit" -> "month", "mod" -> 12)))
    TaskRequest("CDS.timeBin", dataInputs)
  }
}

object SeasonalCycleRequest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "time" -> Map("start" -> 0, "end" -> 36, "system" -> "indices"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "period" -> 3, "unit" -> "month", "mod" -> 4, "offset" -> 2)))
    TaskRequest("CDS.timeBin", dataInputs)
  }
}

object YearlyMeansRequest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "period" -> 12, "unit" -> "month")))
    TaskRequest("CDS.timeBin", dataInputs)
  }
}

object SubsetRequest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
        Map("name" -> "d1", "time" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "domain" -> "d1")))
    TaskRequest("CDS.subset", dataInputs)
  }
}

object TimeSliceAnomaly extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
    TaskRequest("CDS.anomaly", dataInputs)
  }
}

object MetadataRequest extends SyncExecutor {
  val level = 0
  def getTaskRequest: TaskRequest = {
    val dataInputs: Map[String, Seq[Map[String, Any]]] = level match {
      case 0 => Map()
      case 1 => Map("variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0")))
    }
    TaskRequest("CDS.metadata", dataInputs)
  }
}

object CacheRequest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra300/hourly/asm_Cp", "name" -> "t:v0", "domain" -> "d0")))
    TaskRequest("util.cache", dataInputs)
  }
}

object AggregateAndCacheRequest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggTest", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/", "name" -> "t", "domain" -> "d0")))
    TaskRequest("util.cache", dataInputs)
  }
}

object AggregateAndCacheRequest2 extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/daily/aggTest", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", "name" -> "t", "domain" -> "d0")))
    TaskRequest("util.cache", dataInputs)
  }
}

object AggregateAndCacheRequest1 extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra2/hourly/M2T1NXLND-2004-04", "path" -> "/att/pubrepo/MERRA/remote/MERRA2/M2T1NXLND.5.12.4/2004/04", "name" -> "SFMC", "domain" -> "d0")))
    TaskRequest("util.cache", dataInputs)
  }
}

object Max extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "xy")))
    TaskRequest("CDS.max", dataInputs)
  }
}

object Min extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "xy")))
    TaskRequest("CDS.min", dataInputs)
  }
}

object AnomalyTest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> -7.0854263, "end" -> -7.0854263, "system" -> "values"), "lon" -> Map("start" -> 12.075, "end" -> 12.075, "system" -> "values"), "lev" -> Map("start" -> 1000, "end" -> 1000, "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggtest", "name" -> "t:v0", "domain" -> "d0")), // collection://merra300/hourly/asm_Cp
      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
    TaskRequest("CDS.anomaly", dataInputs)
  }
}

object AnomalyTest1 extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 20.0, "end" -> 20.0, "system" -> "values"), "lon" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://merra2/hourly/m2t1nxlnd-2004-04", "name" -> "SFMC:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
    TaskRequest("CDS.anomaly", dataInputs)
  }
}
object AnomalyTest2 extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"), "lon" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"), "level" -> Map("start" -> 10, "end" -> 10, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/daily/aggTest", "name" -> "t:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
    TaskRequest("CDS.anomaly", dataInputs)
  }
}

object AnomalyArrayTest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d1", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")), Map("name" -> "d0", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lon" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lev" -> Map("start" -> 30, "end" -> 30, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "t", "name" -> "CDS.anomaly"), Map("input" -> "v0", "domain" -> "d1", "name" -> "CDS.subset")))
    TaskRequest("CDS.workflow", dataInputs)
  }
}

object AnomalyArrayNcMLTest extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d1", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")), Map("name" -> "d0", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lon" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lev" -> Map("start" -> 30, "end" -> 30, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "file://Users/tpmaxwel/data/AConaty/comp-ECMWF/ecmwf.xml", "name" -> "Temperature:v0", "domain" -> "d0")),
      "operation" -> List(Map("input" -> "v0", "axes" -> "t", "name" -> "CDS.anomaly"), Map("input" -> "v0", "domain" -> "d1", "name" -> "CDS.subset")))
    TaskRequest("CDS.workflow", dataInputs)
  }
}

object AveArray extends SyncExecutor {
  def getTaskRequest: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer](new WorkflowContainer(operations = List(new OperationContext(identifier = "CDS.average~ivar#1", name = "CDS.average", rid = "ivar#1", inputs = List("v0"), Map("axis" -> "xy")))))
    val variableMap = Map[String, DataContainer]("v0" -> new DataContainer(uid = "v0", source = Some(new DataSource(name = "hur", collection = getCollection("merra/mon/atmos"), domain = "d0"))))
    val domainMap = Map[String, DomainContainer]("d0" -> new DomainContainer(name = "d0", axes = cdsutils.flatlist(DomainAxis(Z, 4, 4), DomainAxis(Y, 100, 100)), None))
    new TaskRequest("CDS.average", variableMap, domainMap, workflows, Map("id" -> "v0"))
  }
}

object displayFragmentMap extends App {
  val entries: Seq[(DataFragmentKey,String)] = FragmentPersistence.getEntries.map { case (key, value) => ( DataFragmentKey(key), value ) }
  entries.foreach { case (dkey, cache_id) => println( "%s => %s".format( cache_id, dkey.toString ))}
}

object SpatialAve1 extends SyncExecutor {
  def getTaskRequest: TaskRequest = SampleTaskRequests.getSpatialAve("/MERRA/mon/atmos", "ta", "cosine")
}


object execConstantTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val async = true
  val run_args = Map( "async" -> async.toString )
  val request = SampleTaskRequests.getConstant( "/MERRA/mon/atmos", "ta", 10 )
  if(async) {
    cds2ExecutionManager.executeAsync(request, run_args) match {
      case ( resultId: String, futureResult: Future[ExecutionResults] ) =>
        val result = Await.result (futureResult, Duration.Inf)
        println(">>>> Async Result: " + result )
      case x => println( "Unrecognized result from executeAsync: " + x.toString )
    }
  }
  else {
    val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
    val printer = new scala.xml.PrettyPrinter(200, 3)
    println(">>>> Final Result: " + printer.format(final_result.toXml))
  }
}

object execTestDataCreation extends App {
  SampleTaskRequests.createTestData
}

object parseTest extends App {
  val axes = "c,,,"
  val r = axes.split(",").map(_.head).toList
  println( r )
}




//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContext { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })

