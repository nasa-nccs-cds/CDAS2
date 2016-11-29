package nasa.nccs.cds2.engine

import nasa.nccs.caching.{RDDTransientVariable, collectionDataCache}
import nasa.nccs.cdapi.cdm.{EmptyOperationInput, OperationInput, OperationTransientInput}
import nasa.nccs.cdapi.kernels.{CDASExecutionContext, Kernel}
import nasa.nccs.esgf.process.{OperationContext, RequestContext, ServerContext, TaskRequest}
import nasa.nccs.esgf.process.OperationContext.{OpResultType, ResultType}
import nasa.nccs.utilities.Loggable

object WorkflowNode {
  def apply( context: CDASExecutionContext, kernel: Kernel ): WorkflowNode = {
    new WorkflowNode( context, kernel )
  }
}

class WorkflowNode( val context: CDASExecutionContext, val kernel: Kernel  ) {
  import scala.collection.mutable.HashMap
  private val dependencies = new HashMap[String,WorkflowNode]()
  def getResultType: OpResultType = context.operation.resultType
  def getResultId: String = context.operation.rid
  def getNodeId(): String = context.operation.identifier
  def addDependency( node: WorkflowNode ) = { dependencies.update( node.getNodeId(), node ) }
}

class Workflow( val nodes: List[WorkflowNode], val serverContext: ServerContext ) extends Loggable {

  def getNodeInputs( workflowNode: WorkflowNode ): Map[String,OperationInput] = {
    val items = for (uid <- workflowNode.context.operation.inputs) yield {
      workflowNode.context.request.getInputSpec(uid) match {
        case Some(inputSpec) =>
          logger.info("getInputSpec: %s -> %s ".format(uid, inputSpec.longname))
          uid -> workflowNode.context.server.getOperationInput(inputSpec)
        case None =>
          nodes.find( _.getResultId.equals(uid) ) match {
            case Some( inode ) =>
              workflowNode.addDependency( inode )
              getNodeInputs( inode )
            case None =>
              val errorMsg = "Unidentified input in workflow node %s: %s".format( workflowNode.getNodeId, uid )
              logger.error( errorMsg )
              throw new Exception( errorMsg )
          }
          uid -> new EmptyOperationInput()
      }
    }
    Map(items:_*)
  }

  def stream( request: TaskRequest, requestCx: RequestContext ): Unit = {
    val product_nodes = nodes.filter( _.getResultType == ResultType.PRODUCT )
    for( product_node <- product_nodes ) {
      val inputs = getNodeInputs( product_node )
    }
  }


}
