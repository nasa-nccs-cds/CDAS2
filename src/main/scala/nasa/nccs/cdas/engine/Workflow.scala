package nasa.nccs.cdas.engine

import nasa.nccs.caching.{BatchSpec, RDDTransientVariable, collectionDataCache}
import nasa.nccs.cdapi.cdm.{OperationInput, _}
import nasa.nccs.cdapi.data.{RDDRecord, RDDRecord$}
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdas.engine.spark.{RecordKey$, _}
import nasa.nccs.cdas.kernels.Kernel.RDDKeyValPair
import nasa.nccs.cdas.kernels._
import nasa.nccs.cdas.utilities.runtime
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.{DAGNode, Loggable, ProfilingTool}
import nasa.nccs.wps._
import org.apache.spark.rdd.RDD
import ucar.ma2
import ucar.nc2.dataset.CoordinateAxis1DTime

import scala.util.Try

object WorkflowNode {
  val regridKernel = new CDMSRegridKernel()
  def apply( operation: OperationContext, kernel: Kernel  ): WorkflowNode = {
    new WorkflowNode( operation, kernel )
  }
}

class WorkflowNode( val operation: OperationContext, val kernel: Kernel  ) extends DAGNode with Loggable {
  import WorkflowNode._
  def getResultId: String = operation.rid
  def getNodeId(): String = operation.identifier
  protected val _mapByKey = false

  def fatal( msg: String ) = throw new Exception( s"Workflow Node '${operation.identifier}' Error: " + msg )

  def getKernelOption( key: String , default: String = ""): String = kernel.options.getOrElse(key,default)

  def generateKernelContext( requestCx: RequestContext, profiler: ProfilingTool ): KernelContext = {
    val sectionMap: Map[String, Option[CDSection]] = requestCx.inputs.mapValues(_.map(_.cdsection)).map(identity)
    val gridMap: Map[String,Option[GridContext]] = requestCx.getTargetGrids.map { case (uid,tgridOpt) => uid -> tgridOpt.map( tg => GridContext(uid,tg)) }
    new KernelContext( operation, gridMap, sectionMap, requestCx.domains, requestCx.getConfiguration, profiler )
  }

  def reduce(mapresult: RDD[(RecordKey,RDDRecord)], context: KernelContext, batchIndex: Int ): (RecordKey,RDDRecord) = {
    logger.debug( "\n\n ----------------------- BEGIN reduce[%d] Operation: %s (%s): thread(%s) ----------------------- \n".format( batchIndex, context.operation.identifier, context.operation.rid, Thread.currentThread().getId ) )
    runtime.printMemoryUsage
    val t0 = System.nanoTime()
    val nparts = mapresult.getNumPartitions
    if( !kernel.parallelizable || (nparts==1) ) { mapresult.collect()(0) }
    else {
      val result = mapresult treeReduce kernel.getReduceOp(context)
      logger.debug("\n\n ----------------------- FINISHED reduce Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
      context.addTimestamp( "FINISHED reduce Operation" )
      result
    }
  }


  //  def collect(mapresult: RDD[(PartitionKey,RDDPartition)], context: KernelContext ): RDDPartition = {
//    logger.info( "\n\n ----------------------- BEGIN collect Operation: %s (%s) ----------------------- \n".format( context.operation.identifier, context.operation.rid ) )
//    val t0 = System.nanoTime()
//    var repart_mapresult = mapresult repartitionAndSortWithinPartitions PartitionManager.getPartitioner(mapresult)
//    val result = repart_mapresult.reduce(kernel.mergeRDD(context) _)._2
//    logger.info("\n\n ----------------------- FINISHED collect Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
//    result
//  }


  def regridRDDElems(input: RDD[(RecordKey,RDDRecord)], context: KernelContext): RDD[(RecordKey,RDDRecord)] =
    input.mapValues( rec => regridKernel.map( context )(rec) ) map identity

  def timeConversion(input: RDD[(RecordKey,RDDRecord)], partitioner: RangePartitioner, context: KernelContext, requestCx: RequestContext ): RDD[(RecordKey,RDDRecord)] = {
    val trsOpt: Option[String] = context.trsOpt
    val gridMap: Map[String,TargetGrid] = Map( (for( uid: String <- context.operation.inputs; targetGrid: TargetGrid = requestCx.getTargetGrid(uid).getOrElse( fatal("Missing target grid for kernel input " + uid) ) ) yield  uid -> targetGrid ) : _* )
    val targetTrsGrid: TargetGrid = trsOpt match {
      case Some( trs ) =>
        val trs_input = context.operation.inputs.find( _.split('-')(0).equals( trs.substring(1) ) ).getOrElse( fatal( "Invalid trs configuration: " + trs ) )
        gridMap.getOrElse( trs_input, fatal( "Invalid trs configuration: " + trs ) )
      case None => gridMap.values.head
    }
    val toAxis: CoordinateAxis1DTime = targetTrsGrid.getTimeCoordinateAxis.getOrElse( fatal( "Missing time axis for configuration: " + trsOpt.getOrElse("None") ) )
    val toAxisRange: ma2.Range = targetTrsGrid.getFullSection.getRange(0)
    val new_partitioner: RangePartitioner = partitioner.colaesce
    val conversionGridMap: Map[String,TargetGrid] = gridMap.filter { case (uid, grid) => grid.shape(0) != toAxis.getSize }
    val fromAxisMap: Map[ Int, CoordinateAxis1DTime ] =  conversionGridMap map { case (uid, grid) => grid.shape(0) ->
      requestCx.getTargetGrid(uid).getOrElse(throw new Exception("Missing Target Grid: " + uid))
        .getTimeCoordinateAxis.getOrElse(throw new Exception("Missing Time Axis: " + uid) )    }
    val conversionMap: Map[Int,TimeConversionSpec] = fromAxisMap mapValues ( fromAxis => { val converter = TimeAxisConverter( toAxis, fromAxis, toAxisRange ); converter.computeWeights(); } ) map (identity)
    CDSparkContext.coalesce( input, context ).map { case ( pkey, rdd_part ) => ( new_partitioner.range, rdd_part.reinterp( conversionMap ) ) } repartitionAndSortWithinPartitions new_partitioner
  }

  def map(input: RDD[(RecordKey,RDDRecord)], context: KernelContext ): RDD[(RecordKey,RDDRecord)] = {
    logger.info( "Executing map OP for Kernel " + kernel.id + ", OP = " + context.operation.identifier )
    if( _mapByKey ) {
      input.reduceByKey( kernel.aggregate(context) )
    } else {
      input.mapValues( kernel.map(context) )
    }
  }

  def disaggPartitions(input: RDD[(RecordKey,RDDRecord)], context: KernelContext ): RDD[(RecordKey,RDDRecord)] = {
    logger.info( "Executing map OP for Kernel " + kernel.id + ", OP = " + context.operation.identifier )
    val keyedInput: RDD[(RecordKey,RDDRecord)] = input.mapPartitionsWithIndex( kernel.keyMapper )
    keyedInput.mapValues( kernel.map(context) )
  }
}



object Workflow {
  def apply( request: TaskRequest, executionMgr: CDS2ExecutionManager ): Workflow = {
    new Workflow( request, executionMgr )
  }
}

class Workflow( val request: TaskRequest, val executionMgr: CDS2ExecutionManager ) extends Loggable {
  val nodes = request.operations.map(opCx => WorkflowNode( opCx, createKernel( opCx.name.toLowerCase ) ) )
  val roots = findRootNodes()

  def createKernel(id: String): Kernel = executionMgr.getKernel(id)

  def generateProduct( requestCx: RequestContext, node: WorkflowNode  ): WPSProcessExecuteResponse = {
    val result = executeKernel( requestCx, node )
    createResponse( result, requestCx, node )
  }

  def executeKernel( requestCx: RequestContext, node: WorkflowNode  ): RDDRecord = {
    val t0 = System.nanoTime()
    val opInputs = getNodeInputs( requestCx, node )
    val kernelContext = node.generateKernelContext( requestCx, requestCx.profiler )
    kernelContext.addTimestamp( s"Executing Kernel for node ${node.getNodeId}" )
    var pre_result: RDDRecord = mapReduce( node, opInputs, kernelContext, requestCx )
    val t1 = System.nanoTime()
    val result = node.kernel.postRDDOp( node.kernel.orderElements( pre_result, kernelContext ), kernelContext  )
    if( Try( requestCx.config("unitTest","false").toBoolean ).getOrElse(false)  ) { node.kernel.cleanUp(); }
    val t2 = System.nanoTime()
    logger.info(s"********** Completed Execution of Kernel[%s(%s)]: %s , total time = %.3f sec, postOp time = %.3f sec   ********** \n".format(node.kernel.name,node.kernel.id, node.operation.identifier, (t2 - t0) / 1.0E9, (t2 - t1) / 1.0E9))
    result
  }

  def executeRequest(requestCx: RequestContext): List[ WPSProcessExecuteResponse ] = {
    linkNodes( requestCx )
    val product_nodes = DAGNode.sort( nodes.filter( _.isRoot ) )
    for (product_node <- product_nodes) yield {
      logger.info( "\n\n ----------------------- Execute PRODUCT Node: %s -------\n".format( product_node.getNodeId() ))
      generateProduct(requestCx, product_node)
    }
  }

  def mapReduceBatch( node: WorkflowNode, opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext, batchIndex: Int ): Option[ ( RecordKey, RDDRecord ) ] = {
    prepareInputs(node, opInputs, kernelContext, requestCx, batchIndex) map (inputs => {
      kernelContext.addTimestamp( s"Executing Map Op, Batch ${batchIndex.toString} for node ${node.getNodeId}", true )
      val mapresult = node.map(inputs, kernelContext)
      logger.info(s"Executing Reduce Op, Batch ${batchIndex.toString} for node ${node.getNodeId}")
      val result: (RecordKey, RDDRecord) = node.reduce(mapresult, kernelContext, batchIndex)
      logger.info(s"Completed Reduce op, result metadata: ${result._2.metadata.mkString(", ")}")
      mapReduceBatch(node, opInputs, kernelContext, requestCx, batchIndex + 1) match {
        case Some(next_result) =>
          val reduceOp = node.kernel.getReduceOp(kernelContext)
          reduceOp(result, next_result)
        case None => result
      }
    })
  }

  def streamMapReduceBatch( node: WorkflowNode, opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext, batchIndex: Int ): Option[RDD[(RecordKey,RDDRecord)]] =
    prepareInputs(node, opInputs, kernelContext, requestCx, batchIndex) map ( inputs => {
      logger.info( s"Executing mapReduce Batch ${batchIndex.toString}" )
      val mapresult = node.map( inputs, kernelContext )
      streamReduceNode( mapresult, node, kernelContext, batchIndex )
      })

  def streamMapReduceBatchRecursive( node: WorkflowNode, opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext, batchIndex: Int ): Option[RDD[(RecordKey,RDDRecord)]] =
    prepareInputs(node, opInputs, kernelContext, requestCx, batchIndex) map ( inputs => {
      logger.info( s"Executing mapReduce Batch ${batchIndex.toString}" )
      val mapresult = node.map( inputs, kernelContext )
      val result: RDD[(RecordKey,RDDRecord)] = streamReduceNode( mapresult, node, kernelContext, batchIndex )
      streamMapReduceBatchRecursive( node, opInputs, kernelContext, requestCx, batchIndex + 1 ) match {
        case Some( next_result ) =>
          val reduceOp = node.kernel.getReduceOp(kernelContext)
          result.join(next_result).mapValues(rdds => node.kernel.combineRDD(kernelContext)(rdds._1, rdds._2))
        case None => result
      }})

  def streamReduceNode(mapresult: RDD[(RecordKey,RDDRecord)], node: WorkflowNode, context: KernelContext, batchIndex: Int ): RDD[(RecordKey,RDDRecord)] = {
    logger.debug( "\n\n ----------------------- BEGIN stream reduce[%d] Operation: %s (%s): thread(%s) ----------------------- \n".format( batchIndex, context.operation.identifier, context.operation.rid, Thread.currentThread().getId ) )
    runtime.printMemoryUsage
    val t0 = System.nanoTime()
    if( context.doesTimeReduction ) {
      val nparts = mapresult.getNumPartitions
      if( !node.kernel.parallelizable || (nparts==1) ) {
        mapresult.mapValues( record => node.kernel.postRDDOp( record, context  ) )
      }
      else {
        val inputNParts = mapresult.partitions.length
        val intermediateNParts: Int = if (context.commutativeReduction) { inputNParts } else { 1 }
        logger.debug( "NPARTS: " + inputNParts + ", " + intermediateNParts )
        val pre_result_pair = mapresult treeReduce node.kernel.getReduceOp(context)
        val result = pre_result_pair._1 -> node.kernel.postRDDOp( pre_result_pair._2, context  )
        logger.debug("\n\n ----------------------- FINISHED stream reduce Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
        val results = List.fill(inputNParts)( result )
        executionMgr.serverContext.spark.sparkContext.parallelize( results )
      }
    } else { mapresult }
  }

  def mapReduce( node: WorkflowNode, opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext ): RDDRecord = {
    mapReduceBatch( node, opInputs, kernelContext, requestCx, 0 ) match {
      case Some( ( key, rddPart ) ) =>
        rddPart.configure("gid", kernelContext.grid.uid)
      case None =>
        throw new Exception( s"No partitions in mapReduce for node ${node.getNodeId}" )
    }
  }

  def stream(node: WorkflowNode, requestCx: RequestContext, batchIndex: Int ): Option[ RDD[ (RecordKey,RDDRecord) ] ] = {
    val opInputs = getNodeInputs( requestCx, node )
    val kernelContext = node.generateKernelContext( requestCx, requestCx.profiler )
    val rv = streamMapReduceBatch( node, opInputs, kernelContext, requestCx, batchIndex )      // TODO: Break stream at time reduction boundaries.
    rv
  }

  def prepareInputs( node: WorkflowNode, opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext, batchIndex: Int ): Option[RDD[(RecordKey,RDDRecord)]] = {
    domainRDDPartition( opInputs, kernelContext, requestCx, node, batchIndex ) match {
      case Some(rdd) =>
        logger.info( s"Prepared inputs with ${rdd.partitions.length} parts for node ${node.getNodeId()}"); Some(rdd)
      case None =>
        logger.info( s"No inputs for node ${node.getNodeId()}"); None
    }
  }

  def linkNodes(requestCx: RequestContext): Unit = {
    logger.info( s"linkNodes; inputs = ${requestCx.inputs.keys.mkString(",")}")
    for (workflowNode <- nodes; uid <- workflowNode.operation.inputs)  {
      requestCx.getInputSpec(uid) match {
        case Some(inputSpec) => Unit
        case None =>
          nodes.find(_.getResultId.equals(uid)) match {
            case Some(inode) => workflowNode.addChild(inode)
            case None =>
              val errorMsg = " * Unidentified input in workflow node %s: '%s': This is typically due to an empty domain intersection with the dataset! \n ----> inputs ids = %s, input source keys = %s, input source values = %s, result ids = %s".format(
                workflowNode.getNodeId, uid, requestCx.inputs.keySet.map(k=>s"'$k'").mkString(", "), requestCx.inputs.keys.mkString(", "), requestCx.inputs.values.mkString(", "),
                nodes.map(_.getNodeId()).map(k=>s"'$k'").mkString(", "))
              logger.error(errorMsg)
              throw new Exception(errorMsg)
          }
      }
    }
  }

  def findRootNodes(): List[WorkflowNode] = {
    import scala.collection.mutable.LinkedHashSet
    val results = LinkedHashSet( nodes:_* )
    for (potentialRootNode <- nodes ) {
       for ( workflowNode <- nodes; uid <- workflowNode.operation.inputs )  {
          if( potentialRootNode.getResultId.equals(uid) ) {
            results.remove(potentialRootNode)
          }
       }
    }
    return results.toList
  }

  def getNodeInputs(requestCx: RequestContext, workflowNode: WorkflowNode): Map[String, OperationInput] = {
    val items = for (uid <- workflowNode.operation.inputs) yield {
      requestCx.getInputSpec(uid) match {
        case Some(inputSpec) =>
          logger.info("getInputSpec: %s -> %s ".format(uid, inputSpec.longname))
           if( workflowNode.kernel.extInputs ) {  uid -> new ExternalDataInput( inputSpec, workflowNode )                       }
           else                                {  uid -> executionMgr.serverContext.getOperationInput(inputSpec, requestCx.getConfiguration, workflowNode )  }
        case None =>
          nodes.find(_.getResultId.equals(uid)) match {
            case Some(inode) =>
              uid -> new DependencyOperationInput( inode, workflowNode )
            case None =>
              val errorMsg = " ** Unidentified input in workflow node %s: %s, input ids = %s".format(workflowNode.getNodeId(), uid, requestCx.inputs.keySet.mkString(", "))
              logger.error(errorMsg)
              throw new Exception(errorMsg)
          }
      }
    }
    Map(items: _*)
  }

  def createResponse(result: RDDRecord, context: RequestContext, node: WorkflowNode ): WPSProcessExecuteResponse = {
    val resultId = cacheResult( result, context, node )
    context.getConf("response","xml") match {
      case "object" =>
        new RefExecutionResult ("WPS", node.kernel, node.operation.identifier, resultId, None )
      case "xml" =>
        new RDDExecutionResult ("WPS", node.kernel, node.operation.identifier, result, resultId) // TODO: serviceInstance
      case "file" =>
        val resultFileOpt: Option[String] = executionMgr.getResultFilePath( resultId )
        new RefExecutionResult( "WPS", node.kernel, node.operation.identifier, resultId, resultFileOpt )
    }
  }

  def cacheResult(result: RDDRecord, context: RequestContext, node: WorkflowNode ): String = {
    collectionDataCache.putResult( node.operation.rid, new RDDTransientVariable( result, node.operation, context ) )
    logger.info( " ^^^^## Cached result, rid = " + node.operation.rid + ", head elem metadata = " + result.elements.head._2.metadata )
    node.operation.rid
  }

  def needsRegrid(rdd: RDD[(RecordKey,RDDRecord)], requestCx: RequestContext, kernelContext: KernelContext ): Boolean = {
    val sampleRDDPart: RDDRecord = rdd.first._2
    val targetGrid = requestCx.getTargetGrid (kernelContext.grid.uid).getOrElse (throw new Exception ("Undefined Target Grid for kernel " + kernelContext.operation.identifier) )
    if( targetGrid.getGridSpec.startsWith("gspec") ) return true
    sampleRDDPart.elements.foreach { case(uid,data) => if( data.gridSpec != targetGrid.getGridSpec ) kernelContext.crsOpt match {
      case Some( crs ) =>
        return true
      case None =>
        requestCx.getTargetGrid(uid) match {
          case Some(tgrid) => if( !tgrid.shape.sameElements( targetGrid.shape ) ) return true
          case None => throw new Exception (s"Undefined Grid in input ${uid} for kernel " + kernelContext.operation.identifier)
        }
    }}
    return false
  }

  def unifyGrids(rdd: RDD[(RecordKey,RDDRecord)], requestCx: RequestContext, kernelContext: KernelContext, node: WorkflowNode  ): RDD[(RecordKey,RDDRecord)] = {
    logger.info( "unifyGrids: OP = " + node.operation.name )
    if( needsRegrid(rdd,requestCx,kernelContext) )
      node.regridRDDElems( rdd, kernelContext.conf(Map("gridSpec"->requestCx.getTargetGridSpec(kernelContext),"crs"->kernelContext.crsOpt.getOrElse(""))))
    else rdd
  }

  def domainRDDPartition( opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode, batchIndex: Int ): Option[RDD[(RecordKey,RDDRecord)]] = {
    val enableRegridding = false
    kernelContext.addTimestamp( "Generating RDD for inputs: " + opInputs.keys.mkString(", "), true )
    val rawRddMap: Map[String,RDD[(RecordKey,RDDRecord)]] = opInputs flatMap { case ( uid, opinput ) => opinput match {
        case ( dataInput: PartitionedFragment) =>
          val opSection: Option[ma2.Section] = getOpSectionIntersection( dataInput.getGrid, node )
          executionMgr.serverContext.spark.getRDD( uid, dataInput, requestCx, opSection, node, batchIndex, kernelContext ) map ( result => uid -> result )
        case ( directInput: CDASDirectDataInput ) =>
          val opSection: Option[ma2.Section] = getOpSectionIntersection( directInput.getGrid, node )
          executionMgr.serverContext.spark.getRDD( uid, directInput, requestCx, opSection, node, batchIndex, kernelContext ) map ( result => uid -> result )
        case ( extInput: ExternalDataInput ) =>
          if( batchIndex > 0 ) { None } else {
            val opSection: Option[ma2.Section] = getOpSectionIntersection( extInput.getGrid, node )
            executionMgr.serverContext.spark.getRDD(uid, extInput, requestCx, opSection, node, kernelContext, batchIndex ) map (result => uid -> result)
          }
        case ( kernelInput: DependencyOperationInput  ) =>
          val keyValOpt = stream( kernelInput.inputNode, requestCx, batchIndex ) map ( result => uid -> result )
          logger.info( "\n\n ----------------------- NODE %s => Stream DEPENDENCY Node: %s, batch = %d, rID = %s, nParts = %d -------\n".format(
            node.getNodeId(), kernelInput.inputNode.getNodeId(), batchIndex, kernelInput.inputNode.getResultId, keyValOpt.map( _._2.partitions.length ).getOrElse(-1) ) )
          keyValOpt
        case (  x ) =>
          throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
      }
    }
    if( rawRddMap.isEmpty ) {
      None
    } else {
      logger.info("\n\n ----------------------- Completed RDD input map[%d], keys: { %s }, thread: %s -------\n".format(batchIndex,rawRddMap.keys.mkString(", "), Thread.currentThread().getId ))
      val unifiedRDD = unifyRDDs(rawRddMap, kernelContext, requestCx, node)
      if( enableRegridding) { Some( unifyGrids(unifiedRDD, requestCx, kernelContext, node) ) }
      else { Some( unifiedRDD ) }
    }
  }

  def unifyRDDs(rddMap: Map[String,RDD[(RecordKey,RDDRecord)]], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ) : RDD[(RecordKey,RDDRecord)] = {
    logger.info( "unifyRDDs: " + rddMap.keys.mkString(", ") )
    val parallelizable = node.kernel.parallelizable
    val convertedRdds = if( node.kernel.extInputs ) { rddMap.values }
    else {
      val rdds = rddMap.values
      val trsRdd: RDD[(RecordKey,RDDRecord)] = kernelContext.trsOpt match {
        case Some(trs) => rddMap.keys.find( _.split('-').dropRight(1).mkString("-").equals(trs.substring(1)) ) match {
          case Some(trsKey) => rddMap.getOrElse(trsKey, throw new Exception( s"Error retreiving key $trsKey from rddMap with keys {${rddMap.keys.mkString(",")}}" ) )
          case None => throw new Exception( s"Unmatched trs $trs in kernel ${kernelContext.operation.name}, keys = {${rddMap.keys.mkString(",")}}" )
        }
        case None => rdds.head
      }
      val tPartitioner = CDSparkContext.getPartitioner(trsRdd)
      rddMap.values map ( rdd => {
        val partitioner = CDSparkContext.getPartitioner(rdd)
        val repart_result = if (partitioner.equals(tPartitioner)) { rdd }
        else {
          val convertedResult = if (CDSparkContext.getPartitioner(rdd).numElems != tPartitioner.numElems) {
            node.timeConversion(rdd, tPartitioner, kernelContext, requestCx)
          } else { rdd }
          CDSparkContext.repartition(convertedResult, tPartitioner)
        }
        if(parallelizable) { repart_result }
        else { CDSparkContext.coalesce(repart_result,kernelContext) }
      })
    }
    if( convertedRdds.size == 1 ) convertedRdds.head
    else convertedRdds.tail.foldLeft( convertedRdds.head )( CDSparkContext.merge )
  }

  //  def domainRDDPartition( opInputs: Map[String,OperationInput], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ): RDD[(Int,RDDPartition)] = {
  //    val targetGrid: TargetGrid = getKernelGrid( kernelContext, requestCx )
  //    val opSection: Option[ma2.Section] = getOpSectionIntersection( targetGrid, node )
  //    val rawRdds: Iterable[RDDRegen] = opInputs.map { case ( uid, opinput ) => opinput match {
  //      case ( dataInput: PartitionedFragment) =>
  //        new RDDRegen( executionMgr.serverContext.spark.getRDD( uid, dataInput, requestCx, opSection, node ), dataInput.getGrid, targetGrid, node, kernelContext )
  //      case ( kernelInput: DependencyOperationInput  ) =>
  //        logger.info( "\n\n ----------------------- Stream DEPENDENCY Node: %s -------\n".format( kernelInput.workflowNode.getNodeId() ))
  //        val ( result, context ) = kernelInput.workflowNode.stream( requestCx )
  //        new RDDRegen( result, getKernelGrid(context,requestCx), targetGrid, node, kernelContext )
  //      case (  x ) =>
  //        throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
  //    }
  //    }
  //    val rawResult: RDD[(Int,RDDPartition)] = if( opInputs.size == 1 ) rawRdds.head._1 else rawRdds.tail.foldLeft( rawRdds.head._1 )( CDSparkContext.merge(_._1,_._1) )
  //    if(needsRegrid) { node.map( rawResult, kernelContext, regridKernel ) } else rawResult
  //  }

  def getOpSections( targetGrid: TargetGrid, node: WorkflowNode ): Option[ IndexedSeq[ma2.Section] ] = {
    val optargs: Map[String, String] = node.operation.getConfiguration
    val domains: IndexedSeq[DomainContainer] = optargs.get("domain") match {
      case Some(domainIds) => domainIds.split(",").flatMap(request.getDomain(_)).toIndexedSeq
      case None => return Some( IndexedSeq.empty[ma2.Section] )
    }
    //    logger.info( "OPT DOMAIN Arg: " + optargs.getOrElse( "domain", "None" ) )
    //    logger.info( "OPT Domains: " + domains.map(_.toString).mkString( ", " ) )
    Some( domains.map(dc => targetGrid.grid.getSubSection(dc.axes) match {
      case Some(section) => section
      case None => return None
    }))
  }

  def getOpSectionIntersection( targetGrid: TargetGrid, node: WorkflowNode): Option[ ma2.Section ] = getOpSections(targetGrid,node) match {
    case None => return None
    case Some( sections ) =>
      if( sections.isEmpty ) None
      else {
        val result = sections.foldLeft(sections.head)( _.intersect(_) )
        if (result.computeSize() > 0) { Some(result) }
        else return None
      }
  }
  def getOpCDSectionIntersection(targetGrid: TargetGrid, node: WorkflowNode): Option[ CDSection ] = getOpSectionIntersection(targetGrid, node).map( CDSection( _ ) )
}


//object SparkTestApp extends App {
//  val nparts = 4
//  def _reduce( rdd: RDD[(Int,Float)], combiner: (Float,Float)=>Float ): RDD[(Int,Float)] = {
//    val mod_rdd = rdd map { case (i,x) => (i/2,x) }
//    val reduced_rdd = mod_rdd.reduceByKey( combiner )
//    if( reduced_rdd.count() > 1 ) _reduce( reduced_rdd, combiner ) else reduced_rdd
//  }
//  val conf = new SparkConf(false).setMaster( s"local[$nparts]" ).setAppName( "SparkTestApp" )
//  val sc = new SparkContext(conf)
//  val rdd: RDD[(Int,Float)] = sc.parallelize( (20 to 0 by -1) map ( i => (i,i.toFloat) ) )
//  val partitioner = new RangePartitioner(nparts,rdd)
//  val ordereddRdd = rdd.partitionBy(partitioner).sortByKey(true)
//  val result = ordereddRdd.collect()
//  println( "\n\n" + result.mkString(", ") + "\n\n" )
//}

