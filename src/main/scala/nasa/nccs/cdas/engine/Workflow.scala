package nasa.nccs.cdas.engine

import nasa.nccs.caching.{RDDTransientVariable, collectionDataCache}
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data.RDDPartition
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdas.engine.spark.{PartitionKey, _}
import nasa.nccs.cdas.kernels.{CDMSRegridKernel, Kernel, KernelContext, zmqPythonKernel}
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.{DAGNode, Loggable}
import nasa.nccs.wps._
import org.apache.spark.rdd.RDD
import ucar.ma2
import ucar.nc2.dataset.CoordinateAxis1DTime

import scala.util.Try

object WorkflowNode {
  val regridKernel = new CDMSRegridKernel()
  def apply( operation: OperationContext, workflow: Workflow ): WorkflowNode = {
    new WorkflowNode( operation, workflow )
  }
}

class WorkflowNode( val operation: OperationContext, val workflow: Workflow  ) extends DAGNode with Loggable {
  import WorkflowNode._
  val kernel = workflow.createKernel( operation.name.toLowerCase )
  def getResultId: String = operation.rid
  def getNodeId(): String = operation.identifier

  def fatal( msg: String ) = throw new Exception( s"Workflow Node '${operation.identifier}' Error: " + msg )

  def getKernelOption( key: String , default: String = ""): String = kernel.options.getOrElse(key,default)

  def generateKernelContext( requestCx: RequestContext ): KernelContext = {
    val sectionMap: Map[String, Option[CDSection]] = requestCx.inputs.mapValues(_.map(_.cdsection)).map(identity)
    val gridMap: Map[String,Option[GridContext]] = requestCx.getTargetGrids.map { case (uid,tgridOpt) => uid -> tgridOpt.map( tg => GridContext(uid,tg)) }
    new KernelContext( operation, gridMap, sectionMap, requestCx.domains, requestCx.getConfiguration)
  }

  def reduce(mapresult: RDD[(PartitionKey,RDDPartition)], context: KernelContext, kernel: Kernel ): RDDPartition = {
    logger.debug( "\n\n ----------------------- BEGIN reduce Operation: %s (%s) ----------------------- \n".format( context.operation.identifier, context.operation.rid ) )
    val t0 = System.nanoTime()
    if( ! kernel.parallelizable ) { mapresult.collect()(0)._2 }
    else {
      val partitioner: RangePartitioner = CDSparkContext.getPartitioner(mapresult).colaesce
      var repart_mapresult = mapresult repartitionAndSortWithinPartitions  partitioner
      val result = repart_mapresult reduce kernel.getReduceOp(context)
      logger.debug("\n\n ----------------------- FINISHED reduce Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
      result._2
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


  def regridRDDElems(input: RDD[(PartitionKey,RDDPartition)], context: KernelContext): RDD[(PartitionKey,RDDPartition)] = {
    val rdd = input.mapValues( rdd_part => regridKernel.map( rdd_part, context ) ) map(identity)
    input.partitioner match { case Some( partitioner ) => rdd partitionBy partitioner; case None => rdd }
  }
  def timeConversion(input: RDD[(PartitionKey,RDDPartition)], partitioner: RangePartitioner, context: KernelContext, requestCx: RequestContext ): RDD[(PartitionKey,RDDPartition)] = {
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
    CDSparkContext.coalesce( input ).map { case ( pkey, rdd_part ) => ( new_partitioner.range, rdd_part.reinterp( conversionMap ) ) } repartitionAndSortWithinPartitions new_partitioner
  }

  def mapReduce( kernelContext: KernelContext, requestCx: RequestContext ): RDDPartition = {
    val inputs = prepareInputs( kernelContext, requestCx )
    val mapresult = map( inputs, kernelContext, kernel )
    val nparts = mapresult.getNumPartitions
    val result = if(nparts == 1) { mapresult.collect()(0)._2 } else { reduce( mapresult, kernelContext, kernel ) }
    result.configure( "gid", kernelContext.grid.uid )
  }

  def stream( requestCx: RequestContext ):  RDD[(PartitionKey,RDDPartition)] = {
    val kernelContext = generateKernelContext( requestCx )
    val inputs = prepareInputs( kernelContext, requestCx )
    map( inputs, kernelContext, kernel )
  }

  def prepareInputs( kernelContext: KernelContext, requestCx: RequestContext ): RDD[(PartitionKey,RDDPartition)] = {
    val opInputs = workflow.getNodeInputs( requestCx, this )
    val inputs = workflow.domainRDDPartition( opInputs, kernelContext, requestCx, this )
    inputs
  }

  def map(input: RDD[(PartitionKey,RDDPartition)], context: KernelContext, kernel: Kernel ): RDD[(PartitionKey,RDDPartition)] = {
    logger.info( "Executing map OP for Kernel " + kernel.id + ", OP = " + context.operation.identifier )
    val rdd = input.mapValues( rdd_part => kernel.map( rdd_part, context ) )
    input.partitioner match { case Some( partitioner ) => rdd partitionBy partitioner; case None => rdd }
  }
}

object Workflow {
  def apply( request: TaskRequest, executionMgr: CDS2ExecutionManager ): Workflow = {
    new Workflow( request, executionMgr )
  }
}

class Workflow( val request: TaskRequest, val executionMgr: CDS2ExecutionManager ) extends Loggable {
  val nodes = request.operations.map(opCx => WorkflowNode(opCx, this))
  val roots = findRootNodes()

  def createKernel(id: String): Kernel = executionMgr.getKernel(id)

  def stream(requestCx: RequestContext): List[ WPSProcessExecuteResponse ] = {
    linkNodes( requestCx )
    val product_nodes = DAGNode.sort( nodes.filter( _.isRoot ) )
    for (product_node <- product_nodes) yield {
      logger.info( "\n\n ----------------------- Execute PRODUCT Node: %s -------\n".format( product_node.getNodeId() ))
      generateProduct(requestCx, product_node)
    }
  }

  def linkNodes(requestCx: RequestContext): Unit = {
    for (workflowNode <- nodes; uid <- workflowNode.operation.inputs)  {
      requestCx.getInputSpec(uid) match {
        case Some(inputSpec) => Unit
        case None =>
          nodes.find(_.getResultId.equals(uid)) match {
            case Some(inode) => workflowNode.addChild(inode)
            case None =>
              val errorMsg = "Unidentified input in workflow node %s: %s, inputs ids = %s, values = %s".format(workflowNode.getNodeId, uid, requestCx.inputs.keySet.mkString(", "), requestCx.inputs.values.mkString(", "))
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
          uid -> executionMgr.serverContext.getOperationInput(inputSpec)
        case None =>
          nodes.find(_.getResultId.equals(uid)) match {
            case Some(inode) =>
              uid -> new DependencyOperationInput(inode)
            case None =>
              val errorMsg = "Unidentified input in workflow node %s: %s, input ids = %s".format(workflowNode.getNodeId, uid, requestCx.inputs.keySet.mkString(", "))
              logger.error(errorMsg)
              throw new Exception(errorMsg)
          }
      }
    }
    Map(items: _*)
  }


  def generateProduct( requestCx: RequestContext, node: WorkflowNode  ): WPSProcessExecuteResponse = {
    val kernelContext = node.generateKernelContext( requestCx )
    val t0 = System.nanoTime()
    var pre_result: RDDPartition = node.mapReduce( kernelContext, requestCx )
    val t1 = System.nanoTime()
    val result = node.kernel.postRDDOp( pre_result, kernelContext  )
    val t2 = System.nanoTime()
    logger.info(s"********** Completed Execution of Kernel[%s(%s)]: %s , total time = %.3f sec, postOp time = %.3f sec   ********** \n".format(node.kernel.name,node.kernel.id, node.operation.identifier, (t2 - t0) / 1.0E9, (t2 - t1) / 1.0E9))
    val response = createResponse( result, requestCx, node )
    if( Try( requestCx.config("unitTest","false").toBoolean ).getOrElse(false)  ) { node.kernel.cleanUp(); }
    response
  }


  def createResponse( result: RDDPartition, context: RequestContext, node: WorkflowNode ): WPSProcessExecuteResponse = {
    val resultId = cacheResult( result, context, node )
    new RDDExecutionResult( "WPS", node.kernel, node.operation.identifier, result, resultId ) // TODO: serviceInstance
  }

  def cacheResult( result: RDDPartition, context: RequestContext, node: WorkflowNode ): String = {
    collectionDataCache.putResult( node.operation.rid, new RDDTransientVariable( result, node.operation, context ) )
 //   logger.info( " ^^^^## Cached result, results = " + collectionDataCache.getResultIdList.mkString(",") + ", shape = " + result.head._2.shape.mkString(",") + ", rid = " + node.operation.rid )
    node.operation.rid
  }

  def needsRegrid( rdd: RDD[(PartitionKey,RDDPartition)], requestCx: RequestContext, kernelContext: KernelContext ): Boolean = {
    val sampleRDDPart: RDDPartition = rdd.first._2
    val targetGrid = requestCx.getTargetGrid (kernelContext.grid.uid).getOrElse (throw new Exception ("Undefined Target Grid for kernel " + kernelContext.operation.identifier) )
    if( targetGrid.getGridSpec.startsWith("gspec") ) return true
    sampleRDDPart.elements.foreach { case(uid,data) => if( data.gridSpec != targetGrid.getGridSpec ) kernelContext.crsOpt match {
      case Some( crs ) => return true
      case None =>
        requestCx.getTargetGrid(uid) match {
          case Some(tgrid) => if( !tgrid.shape.sameElements( targetGrid.shape ) ) return true
          case None => throw new Exception (s"Undefined Grid in input ${uid} for kernel " + kernelContext.operation.identifier)
        }
    }}
    return false
  }

  def unifyGrids( rdd: RDD[(PartitionKey,RDDPartition)], requestCx: RequestContext, kernelContext: KernelContext, node: WorkflowNode  ): RDD[(PartitionKey,RDDPartition)] = {
    logger.info( "unifyGrids: OP = " + node.operation.name )
    if( needsRegrid(rdd,requestCx,kernelContext) )
      node.regridRDDElems( rdd, kernelContext.conf(Map("gridSpec"->requestCx.getTargetGridSpec(kernelContext),"crs"->kernelContext.crsOpt.getOrElse(""))))
    else rdd
  }

  def domainRDDPartition( opInputs: Map[String,OperationInput], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ): RDD[(PartitionKey,RDDPartition)] = {
    logger.info( "Generating RDD for inputs: " + opInputs.keys.mkString(", ") )
    val rawRddMap: Map[String,RDD[(PartitionKey,RDDPartition)]] = opInputs.map { case ( uid, opinput ) => opinput match {
        case ( dataInput: PartitionedFragment) =>
          val opSection: Option[ma2.Section] = getOpSectionIntersection( dataInput.getGrid, node )
          uid -> executionMgr.serverContext.spark.getRDD( uid, dataInput, requestCx, opSection, node )
        case ( kernelInput: DependencyOperationInput  ) =>
          logger.info( "\n\n ----------------------- Stream DEPENDENCY Node: %s, opID = %s, rID = %s -------\n".format( kernelInput.workflowNode.getNodeId(), kernelInput.workflowNode.operation.identifier, kernelInput.workflowNode.getResultId ))
          uid -> kernelInput.workflowNode.stream( requestCx )
        case (  x ) =>
          throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
      }
    }
    val unifiedRDD = unifyRDDs( rawRddMap, kernelContext, requestCx, node )
    unifyGrids( unifiedRDD, requestCx, kernelContext, node )
  }

  def unifyRDDs( rddMap: Map[String,RDD[(PartitionKey,RDDPartition)]], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ) : RDD[(PartitionKey,RDDPartition)] = {
    logger.info( "unifyRDDs: " + rddMap.keys.mkString(", ") )
    val rdds = rddMap.values
    val trsRdd: RDD[(PartitionKey,RDDPartition)] = kernelContext.trsOpt match {
      case Some(trs) => rddMap.keys.find( _.split('-').dropRight(1).mkString("-").equals(trs.substring(1)) ) match {
        case Some(trsKey) => rddMap.getOrElse(trsKey, throw new Exception( s"Error retreiving key $trsKey from rddMap with keys {${rddMap.keys.mkString(",")}}" ) )
        case None => throw new Exception( s"Unmatched trs $trs in kernel ${kernelContext.operation.name}, keys = {${rddMap.keys.mkString(",")}}" )
      }
      case None => rdds.head
    }
    val tPartitioner = CDSparkContext.getPartitioner(trsRdd)
    val convertedRdds = rddMap.values map( rdd => {
      val partitioner = CDSparkContext.getPartitioner(rdd)
      val repart_result = if (partitioner.equals(tPartitioner)) {
        rdd
      } else {
        val convertedResult = if( CDSparkContext.getPartitioner(rdd).numElems != tPartitioner.numElems ) { node.timeConversion( rdd, tPartitioner, kernelContext, requestCx ) } else { rdd }
        CDSparkContext.repartition( convertedResult, tPartitioner )
      }
      if( node.getKernelOption("parallelize","true").toBoolean ) { repart_result }
      else {  CDSparkContext.coalesce( repart_result ) }
    } )
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

