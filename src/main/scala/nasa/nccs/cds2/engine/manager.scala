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


class Counter(start: Int = 0) {
  private val index = new AtomicReference(start)
  def get: Int = {
    val i0 = index.get
    if(index.compareAndSet( i0, i0 + 1 )) i0 else get
  }
}

object MaskKey {
  def apply( bounds: Array[Double], mask_shape: Array[Int], spatial_axis_indices: Array[Int] ): MaskKey = {
    new MaskKey( bounds, Array( mask_shape(spatial_axis_indices(0)), mask_shape(spatial_axis_indices(1) ) ) )
  }
}
class MaskKey( bounds: Array[Double], dimensions: Array[Int] ) {}

class MetadataOnlyException extends Exception {}

object FragmentPersistence extends DiskCachable with FragSpecKeySet {
  private val fragmentIdCache: Cache[DataFragmentKey,String] = new FutureCache("CacheIdMap","fragment",true)
  def getCacheType = "fragment"

  def persist( fragSpec: DataFragmentSpec, frag: PartitionedFragment ): Future[String] = {
    logger.info( "Persisting Fragment ID for fragment cache recovery: " + fragSpec.toString )
    fragmentIdCache(fragSpec.getKey) { promiseCacheId(frag) _ }
  }

  def getEntries: Seq[(DataFragmentKey,String)] = fragmentIdCache.getEntries

  def promiseCacheId( frag: PartitionedFragment )(p: Promise[String]): Unit =
    try { p.success( bufferToDiskFloat( frag.data.getSectionData ) ) }
    catch { case err: Throwable => logError( err, "Error writing cache data to disk:"); p.failure(err) }

  def restore( cache_id: String, size: Int ): Option[FloatBuffer] = bufferFromDiskFloat( cache_id, size )
  def restore( fragKey: DataFragmentKey ): Option[FloatBuffer] =  fragmentIdCache.get(fragKey).flatMap( restore( _, fragKey.getSize ) )
  def restore( cache_id_future: Future[String], size: Int ): Option[FloatBuffer] = restore( Await.result(cache_id_future, Duration.Inf), size )
  def blockUntilDone(): Unit = Future.sequence( fragmentIdCache.values )

  def deleteEnclosing( fragSpec: DataFragmentSpec ) =
    findEnclosingFragSpecs(  fragmentIdCache.keys, fragSpec.getKey ).foreach( delete )

  def delete( fragKey: DataFragmentKey ) = {
    fragmentIdCache.get(fragKey) match {
      case Some( cache_id_future ) =>
        val path = DiskCacheFileMgr.getDiskCacheFilePath( getCacheType, Await.result( cache_id_future, Duration.Inf ) )
        fragmentIdCache.remove( fragKey )
        if(new java.io.File(path).delete()) logger.info( s"Deleting persisted fragment file '$path', frag: " + fragKey.toString )
        else logger.warn( s"Failed to delete persisted fragment file '$path'" )
      case None => logger.warn( "No Cache ID found for Fragment: " + fragKey.toString )
    }
  }

  def getEnclosingFragmentData( fragSpec: DataFragmentSpec ): Option[ ( DataFragmentKey, FloatBuffer ) ] = {
    val fragKeys = findEnclosingFragSpecs(  fragmentIdCache.keys, fragSpec.getKey )
    fragKeys.headOption match {
      case Some( fkey ) => restore(fkey) match {
        case Some(array) => Some( (fkey->array) )
        case None => None
      }
      case None => None
    }
  }
  def getFragmentData( fragSpec: DataFragmentSpec ): Option[ FloatBuffer ] = restore( fragSpec.getKey )
}


trait FragSpecKeySet extends nasa.nccs.utilities.Loggable {

  def getFragSpecsForVariable(keys: Set[DataFragmentKey], collection: String, varName: String): Set[DataFragmentKey] = keys.filter(
    _ match {
      case fkey: DataFragmentKey => fkey.sameVariable(collection, varName)
      case x => logger.warn("Unexpected fragment key type: " + x.getClass.getName); false
    }).map(_ match { case fkey: DataFragmentKey => fkey })


  def findEnclosingFragSpecs(keys: Set[DataFragmentKey], fkey: DataFragmentKey, admitEquality: Boolean = true): Set[DataFragmentKey] = {
    val variableFrags = getFragSpecsForVariable(keys, fkey.collectionUrl, fkey.varname)
    variableFrags.filter(fkeyParent => fkeyParent.contains(fkey, admitEquality))
  }

  def findEnclosedFragSpecs(keys: Set[DataFragmentKey], fkeyParent: DataFragmentKey, admitEquality: Boolean = false): Set[DataFragmentKey] = {
    val variableFrags = getFragSpecsForVariable(keys, fkeyParent.collectionUrl, fkeyParent.varname)
    variableFrags.filter(fkey => fkeyParent.contains(fkey, admitEquality))
  }

  def findEnclosingFragSpec(keys: Set[DataFragmentKey], fkeyChild: DataFragmentKey, selectionCriteria: FragmentSelectionCriteria.Value, admitEquality: Boolean = true): Option[DataFragmentKey] = {
    val enclosingFragments = findEnclosingFragSpecs(keys, fkeyChild, admitEquality)
    if (enclosingFragments.isEmpty) None
    else Some(selectionCriteria match {
      case FragmentSelectionCriteria.Smallest => enclosingFragments.minBy(_.getRoi.computeSize())
      case FragmentSelectionCriteria.Largest => enclosingFragments.maxBy(_.getRoi.computeSize())
    })
  }
}

class CollectionDataCacheMgr extends nasa.nccs.esgf.process.DataLoader with FragSpecKeySet {
  private val fragmentCache: Cache[DataFragmentKey,PartitionedFragment] = new FutureCache("Store","fragment",false)
  private val datasetCache: Cache[String,CDSDataset] = new FutureCache("Store","dataset",false)
  private val variableCache: Cache[String,CDSVariable] = new FutureCache("Store","variable",false)
  private val maskCache: Cache[MaskKey,CDByteArray] = new FutureCache("Store","mask",false)
  def clearFragmentCache() = fragmentCache.clear

  def makeKey(collection: String, varName: String) = collection + ":" + varName

  def extractFuture[T](key: String, result: Option[Try[T]]): T = result match {
    case Some(tryVal) => tryVal match {
      case Success(x) => x;
      case Failure(t) => throw t
    }
    case None => throw new Exception(s"Error getting cache value $key")
  }

  def getDatasetFuture(collection: Collection, varName: String): Future[CDSDataset] =
    datasetCache(makeKey(collection.url, varName)) { produceDataset(collection, varName) _ }

  def getDataset(collection: Collection, varName: String): CDSDataset = {
    val futureDataset: Future[CDSDataset] = getDatasetFuture(collection, varName)
    Await.result(futureDataset, Duration.Inf)
  }

  private def produceDataset(collection: Collection, varName: String)(p: Promise[CDSDataset]): Unit = {
    val t0 = System.nanoTime()
    val dataset = CDSDataset.load(collection, varName)
    val t1 = System.nanoTime()
    logger.info(" Completed reading dataset (%s:%s), T: %.4f ".format( collection, varName, (t1-t0)/1.0E9 ))
    p.success(dataset)
  }


  private def promiseVariable(collection: Collection, varName: String)(p: Promise[CDSVariable]): Unit =
    getDatasetFuture(collection, varName) onComplete {
      case Success(dataset) =>
        try {
          val t0 = System.nanoTime()
          val variable = dataset.loadVariable(varName)
          val t1 = System.nanoTime()
          logger.info(" Completed reading variable %s, T: %.4f".format( varName, (t1-t0)/1.0E9 ) )
          p.success(variable)
        }
        catch {
          case e: Exception => p.failure(e)
        }
      case Failure(t) => p.failure(t)
    }

  def getVariableFuture(collection: Collection, varName: String): Future[CDSVariable] = variableCache(makeKey(collection.url, varName)) {
    promiseVariable(collection, varName) _
  }

  def getVariable(collection: Collection, varName: String): CDSVariable = {
    val futureVariable: Future[CDSVariable] = getVariableFuture(collection, varName)
    Await.result(futureVariable, Duration.Inf)
  }

  def getVariable(fragSpec: DataFragmentSpec): CDSVariable = getVariable(fragSpec.collection, fragSpec.varname)

  private def cutExistingFragment( fragSpec: DataFragmentSpec, abortSizeFraction: Float=0f ): Option[PartitionedFragment] = {
    val fragOpt = findEnclosingFragSpec( fragmentCache.keys, fragSpec.getKey, FragmentSelectionCriteria.Smallest) match {
      case Some(fkey: DataFragmentKey) => getExistingFragment(fkey) match {
        case Some(fragmentFuture) =>
          if (!fragmentFuture.isCompleted && (fkey.getSize * abortSizeFraction > fragSpec.getSize)) {
            logger.info("Cache Chunk[%s] found but not yet ready, abandoning cache access attempt".format(fkey.shape.mkString(",")))
            None
          } else {
            val fragment = Await.result(fragmentFuture, Duration.Inf)
            Some(fragment.cutNewSubset(fragSpec.roi))
          }
        case None => cutExistingFragment(fragSpec, abortSizeFraction)
      }
      case None => None
    }
    fragOpt match {
      case None =>
        FragmentPersistence.getEnclosingFragmentData(fragSpec) match {
          case Some((fkey, fltBuffer)) =>
            val cdvar: CDSVariable = getVariable(fragSpec.collection, fragSpec.varname )
            val newFragSpec = fragSpec.reSection(fkey)
            val maskOpt = newFragSpec.mask.flatMap( maskId => produceMask( maskId, newFragSpec.getBounds, newFragSpec.getGridShape, cdvar.getTargetGrid( newFragSpec ).getAxisIndices("xy") ) )
            val fragment = new PartitionedFragment( new CDFloatArray( newFragSpec.getShape, fltBuffer, cdvar.missing ), maskOpt, newFragSpec )
            fragmentCache.put( fkey, fragment )
            Some(fragment.cutNewSubset(fragSpec.roi))
          case None => None
        }
      case x => x
    }
  }

  private def promiseFragment( fragSpec: DataFragmentSpec, dataAccessMode: DataAccessMode )(p: Promise[PartitionedFragment]): Unit =
    getVariableFuture( fragSpec.collection, fragSpec.varname )  onComplete {
      case Success(variable) =>
        try {
          val t0 = System.nanoTime()
          val result = fragSpec.targetGridOpt match {
            case Some( targetGrid ) =>
               val maskOpt = fragSpec.mask.flatMap( maskId => produceMask( maskId, fragSpec.getBounds, fragSpec.getGridShape, targetGrid.getAxisIndices("xy") ) )
               targetGrid.loadRoi( variable, fragSpec, maskOpt, dataAccessMode )
             case None =>
               val targetGrid = new TargetGrid( variable, Some(fragSpec.getAxes) )
               val maskOpt = fragSpec.mask.flatMap( maskId => produceMask( maskId, fragSpec.getBounds, fragSpec.getGridShape, targetGrid.getAxisIndices("xy")  ) )
               targetGrid.loadRoi( variable, fragSpec, maskOpt, dataAccessMode )
          }
          logger.info("Completed variable (%s:%s) subset data input in time %.4f sec, section = %s ".format(fragSpec.collection, fragSpec.varname, (System.nanoTime()-t0)/1.0E9, fragSpec.roi ))
          //          logger.info("Data column = [ %s ]".format( ( 0 until result.shape(0) ).map( index => result.getValue( Array(index,0,100,100) ) ).mkString(", ") ) )
          p.success( result )

        } catch { case e: Exception => p.failure(e) }
      case Failure(t) => p.failure(t)
    }

  def produceMask(maskId: String, bounds: Array[Double], mask_shape: Array[Int], spatial_axis_indices: Array[Int]): Option[CDByteArray] = {
    if (Masks.isMaskId(maskId)) {
      val maskFuture = getMaskFuture( maskId, bounds, mask_shape, spatial_axis_indices  )
      val result = Await.result( maskFuture, Duration.Inf )
      logger.info("Loaded mask (%s) data".format( maskId ))
      Some(result)
    } else {
      None
    }
  }

  private def getMaskFuture( maskId: String, bounds: Array[Double], mask_shape: Array[Int], spatial_axis_indices: Array[Int]  ): Future[CDByteArray] = {
    val fkey = MaskKey(bounds, mask_shape, spatial_axis_indices)
    val maskFuture = maskCache( fkey ) { promiseMask( maskId, bounds, mask_shape, spatial_axis_indices ) _ }
    logger.info( ">>>>>>>>>>>>>>>> Put mask in cache: " + fkey.toString + ", keys = " + maskCache.keys.mkString("[",",","]") )
    maskFuture
  }

  private def promiseMask( maskId: String, bounds: Array[Double], mask_shape: Array[Int], spatial_axis_indices: Array[Int] )(p: Promise[CDByteArray]): Unit =
    try {
      Masks.getMask(maskId) match {
        case Some(mask) => mask.mtype match {
          case "shapefile" =>
            val geotools = new GeoTools()
            p.success( geotools.produceMask( mask.getPath, bounds, mask_shape, spatial_axis_indices ) )
          case x => p.failure(new Exception(s"Unrecognized Mask type: $x"))
        }
        case None => p.failure(new Exception(s"Unrecognized Mask ID: $maskId: options are %s".format(Masks.getMaskIds)))
      }
    } catch { case e: Exception => p.failure(e) }

  private def clearRedundantFragments( fragSpec: DataFragmentSpec ) = findEnclosedFragSpecs( fragmentCache.keys, fragSpec.getKey ).foreach( fragmentCache.remove )

  private def getFragmentFuture( fragSpec: DataFragmentSpec, dataAccessMode: DataAccessMode  ): Future[PartitionedFragment] = {
    val fragFuture = fragmentCache( fragSpec.getKey ) { promiseFragment( fragSpec, dataAccessMode ) _ }
    fragFuture onComplete {
      case Success(fragment) =>
        clearRedundantFragments( fragSpec )
        if( dataAccessMode == DataAccessMode.Cache ) FragmentPersistence.persist( fragSpec, fragment )
      case Failure(t) => Unit
    }
    logger.info( ">>>>>>>>>>>>>>>> Put frag in cache: " + fragSpec.toString + ", keys = " + fragmentCache.keys.mkString("[",",","]") + ", dataAccessMode = " + dataAccessMode.toString )
    fragFuture
  }

  def getFragment( fragSpec: DataFragmentSpec, dataAccessMode: DataAccessMode, abortSizeFraction: Float=0f  ): PartitionedFragment = {
    cutExistingFragment(fragSpec, abortSizeFraction) getOrElse {
      val fragmentFuture = getFragmentFuture(fragSpec, dataAccessMode)
      val result = Await.result(fragmentFuture, Duration.Inf)
      logger.info("Loaded variable (%s:%s) subset data, section = %s ".format(fragSpec.collection, fragSpec.varname, fragSpec.roi))
      result
    }
  }

  def getFragmentAsync( fragSpec: DataFragmentSpec, dataAccessMode: DataAccessMode  ): Future[PartitionedFragment] =
    cutExistingFragment(fragSpec) match {
      case Some(fragment) => Future { fragment }
      case None => getFragmentFuture(fragSpec, dataAccessMode)
    }


//  def loadOperationInputFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[OperationInputSpec] = {
//    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
//    variableFuture.flatMap( variable => {
//      val section = variable.getSubSection(domain_container.axes)
//      val fragSpec = variable.createFragmentSpec( section, domain_container.mask )
//      val axisSpecs: AxisIndices = variable.getAxisIndices(dataContainer.getOpSpecs)
//      for (frag <- getFragmentFuture(fragSpec)) yield new OperationInputSpec( fragSpec, axisSpecs)
//    })
//  }
//
//  def loadDataFragmentFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[PartitionedFragment] = {
//    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
//    variableFuture.flatMap( variable => {
//      val section = variable.getSubSection(domain_container.axes)
//      val fragSpec = variable.createFragmentSpec( section, domain_container.mask )
//      for (frag <- getFragmentFuture(fragSpec)) yield frag
//    })
//  }

  def getExistingMask( fkey: MaskKey  ): Option[Future[CDByteArray]] = {
    val rv: Option[Future[CDByteArray]] = maskCache.get( fkey )
    logger.info( ">>>>>>>>>>>>>>>> Get mask from cache: search key = " + fkey.toString + ", existing keys = " + maskCache.keys.mkString("[",",","]") + ", Success = " + rv.isDefined.toString )
    rv
  }

  def getExistingFragment( fkey: DataFragmentKey  ): Option[Future[PartitionedFragment]] = {
    val rv: Option[Future[PartitionedFragment]] = fragmentCache.get( fkey )
    logger.info( ">>>>>>>>>>>>>>>> Get frag from cache: search key = " + fkey.toString + ", existing keys = " + fragmentCache.keys.mkString("[",",","]") + ", Success = " + rv.isDefined.toString )
    rv
  }
}

object collectionDataCache extends CollectionDataCacheMgr()

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
  def getCollection( id: String ): Collection = {
    Collections.findCollection(id) match { case Some(collection) => collection; case None=> throw new Exception(s"Unknown Collection: $id" )}
  }


  def getAveTimeseries: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", rid = "ivar#1", inputs = List("v0"), Map("axis" -> "t") ) ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = getCollection("merra/mon/atmos"), domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Z,1,1), DomainAxis(Y,100,100), DomainAxis(X,100,100) ), None ) )
    new TaskRequest( "CDS.average", variableMap, domainMap, workflows, Map( "id" -> "v0" ) )
  }

  def getTimeAveSlice: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "hur:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"t" ) ))
      TaskRequest( "CDS.average", dataInputs )
  }

  def getYearlyCycleSlice: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "period"->1, "unit"->"month", "mod"->12  ) ))
    TaskRequest( "CDS.bin", dataInputs )
  }

  def getCreateVRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
                        Map("name" -> "d1", "time" -> Map("start" -> "2010-01-16T12:00:00", "end" -> "2010-01-16T12:00:00", "system" -> "values") ) ),
      "variable" -> List( Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0") ),
      "operation" -> List(  Map( "input"->"v0", "axes"->"t", "name"->"CDS.anomaly" ), Map( "input"->"v0", "period"->1, "unit"->"month", "mod"->12, "name"->"CDS.timeBin"  ), Map( "input"->"v0", "domain"->"d1", "name"->"CDS.subset" ) ) )
    TaskRequest( "CDS.workflow", dataInputs )
  }

  def getYearlyCycleRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List( Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0") ),
      "operation" -> List( Map( "input"->"v0", "period"->1, "unit"->"month", "mod"->12 ) ))
    TaskRequest( "CDS.timeBin", dataInputs )
  }

  def getSeasonalCycleRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "time" -> Map("start" -> 0, "end" -> 36, "system" -> "indices"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List( Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0") ),
      "operation" -> List( Map( "input"->"v0", "period"->3, "unit"->"month", "mod"->4, "offset"->2  ) ))
    TaskRequest( "CDS.timeBin", dataInputs )
  }

  def getYearlyMeansRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List( Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0") ),
      "operation" -> List( Map( "input"->"v0", "period"->12, "unit"->"month" ) ))
    TaskRequest( "CDS.timeBin", dataInputs )
  }

  def getSubsetRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
        Map("name" -> "d1", "time" -> Map("start" -> 3, "end" -> 3, "system" -> "indices") ) ),
      "variable" -> List( Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0") ),
      "operation" -> List( Map( "input"->"v0", "domain"->"d1" ) ))
    TaskRequest( "CDS.subset", dataInputs )
  }

  def getTimeSliceAnomaly: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"t" ) ))
    TaskRequest( "CDS.anomaly", dataInputs )
  }

  def getMetadataRequest( level: Int ): TaskRequest = {
    val dataInputs: Map[String, Seq[Map[String, Any]]] = level match {
      case 0 => Map()
      case 1 => Map( "variable" -> List ( Map( "uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0" ) ) )
    }
    TaskRequest( "CDS.metadata", dataInputs )
  }

  def getCacheRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0",  "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra300/hourly/asm_Cp", "name" -> "t:v0", "domain" -> "d0")) )
    TaskRequest( "util.cache", dataInputs )
  }

  def getAggregateAndCacheRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0",  "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggTest", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/", "name" -> "t", "domain" -> "d0")) )
    TaskRequest( "util.cache", dataInputs )
  }

  def getAggregateAndCacheRequest2: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0",  "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/daily/aggTest", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", "name" -> "t", "domain" -> "d0")) )
    TaskRequest( "util.cache", dataInputs )
  }

  def getAggregateAndCacheRequest1: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0",  "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra2/hourly/M2T1NXLND-2004-04", "path" -> "/att/pubrepo/MERRA/remote/MERRA2/M2T1NXLND.5.12.4/2004/04", "name" -> "SFMC", "domain" -> "d0")) )
    TaskRequest( "util.cache", dataInputs )
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

  def getMax: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"xy" ) ))
    TaskRequest( "CDS.max", dataInputs )
  }

  def getMin: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"xy" ) ))
    TaskRequest( "CDS.min", dataInputs )
  }

  def getAnomalyTest: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List(Map("name" -> "d0", "lat" -> Map("start" -> -7.0854263, "end" -> -7.0854263, "system" -> "values"), "lon" -> Map("start" -> 12.075, "end" -> 12.075, "system" -> "values"), "lev" -> Map("start" -> 1000, "end" -> 1000, "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggtest", "name" -> "t:v0", "domain" -> "d0")),  // collection://merra300/hourly/asm_Cp
      "operation" -> List( Map( "input"->"v0", "axes"->"t" ) ))
    TaskRequest( "CDS.anomaly", dataInputs )
  }
  def getAnomalyTest1: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List(Map("name" -> "d0", "lat" -> Map("start" -> 20.0, "end" -> 20.0, "system" -> "values"), "lon" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://merra2/hourly/m2t1nxlnd-2004-04", "name" -> "SFMC:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"t" ) ))
    TaskRequest( "CDS.anomaly", dataInputs )
  }
  def getAnomalyTest2: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List(Map("name" -> "d0", "lat" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"), "lon" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"), "level" -> Map("start" -> 10, "end" -> 10, "system" -> "indices") )),
      "variable" -> List(Map("uri" -> "collection://merra/daily/aggTest", "name" -> "t:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"t" ) ))
    TaskRequest( "CDS.anomaly", dataInputs )
  }
  def getAnomalyArrayTest: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List( Map("name" -> "d1", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")), Map("name" -> "d0", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lon" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lev" -> Map("start" -> 30, "end" -> 30, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"t", "name"->"CDS.anomaly" ), Map( "input"->"v0", "domain"->"d1", "name"->"CDS.subset" )) )
    TaskRequest( "CDS.workflow", dataInputs )
  }

  def getAnomalyArrayNcMLTest: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List( Map("name" -> "d1", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")), Map("name" -> "d0", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lon" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lev" -> Map("start" -> 30, "end" -> 30, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "file://Users/tpmaxwel/data/AConaty/comp-ECMWF/ecmwf.xml", "name" -> "Temperature:v0", "domain" -> "d0")),
      "operation" -> List( Map( "input"->"v0", "axes"->"t", "name"->"CDS.anomaly" ), Map( "input"->"v0", "domain"->"d1", "name"->"CDS.subset" )) )
    TaskRequest( "CDS.workflow", dataInputs )
  }

  def getAveArray: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", rid = "ivar#1", inputs = List("v0"), Map("axis" -> "xy")  ) ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = getCollection("merra/mon/atmos"), domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Z,4,4), DomainAxis(Y,100,100) ), None ) )
    new TaskRequest( "CDS.average", variableMap, domainMap, workflows, Map( "id" -> "v0" ) )
  }
}

//object exeSyncTest extends App {
//  import nasa.nccs.esgf.process.DomainAxis.Type._
//  val operationContainer =  new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), Map("axis" -> "xy") )
//  val dataContainer = new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) )
//  val domainContainer = new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Z,6,6) ), None )
//  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
//  val t0 = System.nanoTime
//  val partitionedFragmentOpt = SampleTaskRequests.getFragmentSync( dataContainer, domainContainer )
//  val t1 = System.nanoTime
//  partitionedFragmentOpt match {
//    case Some( partitionedFragment ) => println( "Got Value, time = %.4f: %s: ".format( (t1-t0)/1.0E9, partitionedFragment.toString ) )
//    case None => println( "Error" )
//  }
//}
//
//object exeConcurrencyTest extends App {
//  import nasa.nccs.esgf.process.DomainAxis.Type._
//  val operationContainer =  new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), Map("axis" -> "xy") )
//  val dataContainer = new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) )
//  val domainContainer = new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Z,10,10) ), None )
//  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
//  cds2ExecutionManager.serverContext.dataLoader.getVariable( dataContainer.getSource.collection, dataContainer.getSource.name )
//  val t0 = System.nanoTime
////  val futurePartitionedFragment: Future[PartitionedFragment] = cds2ExecutionManager.serverContext.dataLoader.loadDataFragmentFuture( dataContainer, domainContainer )
//  val futurePartitionedFragment: Future[PartitionedFragment]  = SampleTaskRequests.getFragmentSyncFuture( dataContainer, domainContainer )
//  val t1 = System.nanoTime
//  println("Got Future, time = %.4f".format((t1-t0)/1.0E9))
//  val partitionedFragment: PartitionedFragment = Await.result( futurePartitionedFragment, Duration.Inf )
//  val t2 = System.nanoTime
//  println( "Got Value, time = %.4f (%.4f): %s: ".format( (t2-t1)/1.0E9, (t2-t0)/1.0E9,partitionedFragment.toString ) )
//}

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


object execAggregateAndCacheTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAggregateAndCacheRequest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
  FragmentPersistence.blockUntilDone()
}

object execAggregateAndCacheTest1 extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAggregateAndCacheRequest1
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
  FragmentPersistence.blockUntilDone()
}

object execAggregateAndCacheTest2 extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAggregateAndCacheRequest2
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
  FragmentPersistence.blockUntilDone()
}

object execMetadataTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getMetadataRequest(1)
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object execAnomalyTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAnomalyTest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}
object execAnomalyTest1 extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAnomalyTest1
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}
object execAnomalyTest2 extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAnomalyTest2
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object displayFragmentMap extends App {
  val entries: Seq[(DataFragmentKey,String)] = FragmentPersistence.getEntries
  entries.foreach { case (dkey, cache_id) => println( "%s => %s".format( cache_id, dkey.toString ))}
}

object execAnomalyNcMLTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAnomalyArrayNcMLTest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object execCreateVRequest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getCreateVRequest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object execYearlyCycleRequest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getYearlyCycleRequest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val result_xml = final_result.toXml
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(result_xml) )
}

object execSeasonalCycleRequest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getSeasonalCycleRequest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val result_xml = final_result.toXml
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(result_xml) )
}

object execYearlyMeansRequest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getYearlyMeansRequest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val result_xml = final_result.toXml
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(result_xml) )
}

object execSubsetRequest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getSubsetRequest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object execSpatialAveTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getSpatialAve(  "/MERRA/mon/atmos", "ta", "cosine"  )
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object execMaskedSpatialAveTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getMaskedSpatialAve( "/MERRA/mon/atmos", "ta", "cosine" )
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
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

object execMaxTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getMax
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object execMinTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getMin
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object execAnomalyWithCacheTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )

  println( ">>>>>>>>>>>>>>>>>>>>>>>>>> Start CACHE REQUEST "  )
  val t1 = System.nanoTime
  val cache_request = SampleTaskRequests.getCacheRequest
  val cache_result = cds2ExecutionManager.blockingExecute(cache_request, run_args)
  val t2 = System.nanoTime
  println( ">>>>>>>>>>>>>>>>>>>>>>>>>> Cache1: %.4f".format((t2-t1)/1.0E9) )

  val cache_request1 = SampleTaskRequests.getCacheRequest
  val cache_result1 = cds2ExecutionManager.blockingExecute(cache_request, run_args)
  val t3 = System.nanoTime
  println( ">>>>>>>>>>>>>>>>>>>>>>>>>> Cache2: %.4f".format((t3-t2)/1.0E9) )


  val request = SampleTaskRequests.getAnomalyTest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result.toXml) )
}

object parseTest extends App {
  val axes = "c,,,"
  val r = axes.split(",").map(_.head).toList
  println( r )
}





//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContext { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })

