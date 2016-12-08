package nasa.nccs.cds2.engine

import nasa.nccs.caching.{RDDTransientVariable, collectionDataCache}
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data.RDDPartition
import nasa.nccs.cdapi.kernels.{CDASExecutionContext, Kernel, KernelContext}
import nasa.nccs.cds2.engine.spark.CDSparkContext
import nasa.nccs.esgf.process._
import nasa.nccs.esgf.process.OperationContext.{OpResultType, ResultType}
import nasa.nccs.utilities.{ Loggable, DAGNode }
import nasa.nccs.wps._
import org.apache.spark.rdd.RDD
import ucar.ma2
import ucar.ma2.Section

import scala.util.Try

object WorkflowNode {
  def apply( operation: OperationContext, workflow: Workflow ): WorkflowNode = {
    new WorkflowNode( operation, workflow )
  }
}

class WorkflowNode( val operation: OperationContext, val workflow: Workflow  ) extends DAGNode with Loggable {
  val kernel = workflow.createKernel( operation.name.toLowerCase )
  def getResultId: String = operation.rid
  def getNodeId(): String = operation.identifier

  def generateKernelContext( requestCx: RequestContext ): KernelContext = {
    val sectionMap: Map[String, Option[CDSection]] = requestCx.inputs.mapValues(_.map(_.cdsection)).map(identity)
    new KernelContext( operation, GridContext(requestCx.targetGrid), sectionMap, requestCx.getConfiguration)
  }

  def reduce( mapresult: RDD[(Int,RDDPartition)], context: KernelContext, kernel: Kernel ): RDDPartition = {
    logger.info( "\n\n ----------------------- BEGIN reduce Operation: %s (%s) ----------------------- \n".format( context.operation.identifier, context.operation.rid ) )
    val t0 = System.nanoTime()
    val result = if( kernel.reduceCombineOpt.isDefined && context.getAxes.includes(0) ) {
      mapresult.reduce( kernel.reduceRDDOp(context) _ )._2
    } else {
      val results: Seq[(Int, RDDPartition)] =  mapresult.collect().toSeq.sortWith(_._1 < _._1)
      val t1 = System.nanoTime()
      logger.info( "REDUCE STAGES >>>===> Collect: %.3f sec".format( (t1 - t0) / 1.0E9 ))
      results.tail.foldLeft( results.head._2 )( { case (r0,(index,r1)) => kernel.mergeRDD(r0,r1) } )
    }
    logger.info( "\n\n ----------------------- FINISHED reduce Operation: %s (%s), time = %.3f sec ----------------------- ".format( context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
    result
  }

  def mapReduce( kernelContext: KernelContext, requestCx: RequestContext ): RDDPartition = {
    val inputs = prepareInputs( kernelContext, requestCx )
    val mapresult = map( inputs, kernelContext, kernel )
    reduce( mapresult, kernelContext, kernel )
  }

  def stream( requestCx: RequestContext ): RDD[(Int,RDDPartition)] = {
    val kernelContext = generateKernelContext( requestCx )
    val inputs = prepareInputs( kernelContext, requestCx )                                                     // TODO: Add (reduce/broadcast)-when-required
    map( inputs, kernelContext, kernel )
  }

  def prepareInputs( kernelContext: KernelContext, requestCx: RequestContext ): RDD[(Int,RDDPartition)] = {
    val t0 = System.nanoTime()
    val opInputs = workflow.getNodeInputs( requestCx, this )
    val inputs: RDD[(Int,RDDPartition)] = workflow.domainRDDPartition( opInputs, kernelContext, requestCx, this )
    logger.info( " FINISHED defining prepare Inputs, time = %.3f sec".format((System.nanoTime() - t0) / 1.0E9))
    inputs
  }

  def map( input: RDD[(Int,RDDPartition)], context: KernelContext, kernel: Kernel ): RDD[(Int,RDDPartition)] = {
    val t0 = System.nanoTime()
    val result = input.map( rdd_part => kernel.map( rdd_part, context ) )
    logger.info( "FINISHED defining map Operation, time = %.3f sec".format((System.nanoTime() - t0) / 1.0E9))
    result
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

  def stream(requestCx: RequestContext): List[ WPSExecuteResponse ] = {
    linkNodes( requestCx )
    val product_nodes = DAGNode.sort( nodes.filter( _.isRoot ) )
    for (product_node <- product_nodes) yield {
      try {
        logger.info( "\n\n ----------------------- Execute PRODUCT Node: %s -------\n".format( product_node.getNodeId() ))
        generateProduct(requestCx, product_node)
      } catch {
        case err: Exception => createErrorReport( err, requestCx, product_node )
      }
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


  def generateProduct( requestCx: RequestContext, node: WorkflowNode  ): WPSExecuteResponse = {
    val kernelContext = node.generateKernelContext( requestCx )
    val t0 = System.nanoTime()
    var pre_result: RDDPartition = node.mapReduce( kernelContext, requestCx )
    val t1 = System.nanoTime()
    val result = node.kernel.postRDDOp( pre_result, kernelContext  )
    val t2 = System.nanoTime()
    logger.info(s"********** Completed Execution of Kernel[%s(%s)]: %s , total time = %.3f sec, postOp time = %.3f sec   ********** \n".format(node.kernel.name,node.kernel.id, node.operation.identifier, (t2 - t0) / 1.0E9, (t2 - t1) / 1.0E9))
    //    logger.info( "\n\nResult partition elements= %s \n\n".format( result.elements.values.map( cdsutils.toString(_) ) ) )
    val response = createResponse( result, requestCx, node )
    if( Try( requestCx.config("unitTest","false").toBoolean ).getOrElse(false)  ) { node.kernel.cleanUp(); }
    response
  }


  def createResponse( result: RDDPartition, context: RequestContext, node: WorkflowNode ): WPSExecuteResponse = {
    val resultId = cacheResult( result, context, node )
    new RDDExecutionResult( "", node.kernel, node.operation.identifier, result, resultId ) // TODO: serviceInstance
  }

  def createErrorReport( err: Throwable, context: RequestContext, node: WorkflowNode ): WPSExecuteResponse = {
    new ExecutionErrorReport( "", node.kernel, node.operation.identifier, err ) // TODO: serviceInstance
  }

  def cacheResult( result: RDDPartition, context: RequestContext, node: WorkflowNode ): Option[String] = {
    try {
      collectionDataCache.putResult( node.operation.rid, new RDDTransientVariable( result, node.operation, context ) )
      logger.info( " ^^^^## Cached result, results = " + collectionDataCache.getResultIdList.mkString(",") + ", shape = " + result.head._2.shape.mkString(",") + ", rid = " + node.operation.rid )
      Some(node.operation.rid)
    } catch {
      case ex: Exception => logger.error( "Can't cache result: " + ex.getMessage ); None
    }
  }

  def domainRDDPartition( opInputs: Map[String,OperationInput], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ): RDD[(Int,RDDPartition)] = {
    val opSection: Option[ma2.Section] = getOpSectionIntersection( requestCx, node )
    val rdds: Iterable[RDD[(Int,RDDPartition)]] = opInputs.map { case ( uid, opinput ) => opinput match {
        case ( dataInput: PartitionedFragment) =>
          executionMgr.serverContext.spark.getRDD( uid, dataInput, dataInput.partitions, opSection )
        case ( kernelInput: DependencyOperationInput  ) =>
          logger.info( "\n\n ----------------------- Stream DEPENDENCY Node: %s -------\n".format( kernelInput.workflowNode.getNodeId() ))
          kernelInput.workflowNode.stream( requestCx )
        case (  x ) =>
          throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
      }
    }
    if( opInputs.size == 1 ) rdds.head else rdds.tail.foldLeft( rdds.head )( CDSparkContext.merge(_,_) )
  }

  def getOpSections( request: RequestContext, node: WorkflowNode ): Option[ IndexedSeq[ma2.Section] ] = {
    val optargs: Map[String, String] = node.operation.getConfiguration
    val domains: IndexedSeq[DomainContainer] = optargs.get("domain") match {
      case Some(domainIds) => domainIds.split(",").map(request.getDomain(_))
      case None => return Some( IndexedSeq.empty[ma2.Section] )
    }
    //    logger.info( "OPT DOMAIN Arg: " + optargs.getOrElse( "domain", "None" ) )
    //    logger.info( "OPT Domains: " + domains.map(_.toString).mkString( ", " ) )
    Some( domains.map(dc => request.targetGrid.grid.getSubSection(dc.axes) match {
      case Some(section) => section
      case None => return None
    }))
  }

  def getOpSectionIntersection(request: RequestContext, node: WorkflowNode): Option[ ma2.Section ] = getOpSections(request,node) match {
    case None => return None
    case Some( sections ) =>
      if( sections.isEmpty ) None
      else {
        val result = sections.foldLeft(sections.head)( _.intersect(_) )
        if (result.computeSize() > 0) { Some(result) }
        else return None
      }
  }
  def getOpCDSectionIntersection(request: RequestContext, node: WorkflowNode): Option[ CDSection ] = getOpSectionIntersection(request, node).map( CDSection( _ ) )
}
