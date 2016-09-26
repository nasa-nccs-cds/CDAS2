package nasa.nccs.cds2.engine.futures

import nasa.nccs.caching.collectionDataCache
import nasa.nccs.cdapi.cdm.OperationInput
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.engine.CDS2ExecutionManager
import nasa.nccs.esgf.process.DataFragment
import ucar.{ma2, nc2}
import ucar.nc2.Attribute

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class CDFuturesExecutionManager( serverConfig: Map[String,String] = Map.empty ) extends CDS2ExecutionManager(serverConfig) {

  def mapReduce(context: CDASExecutionContext, kernel: Kernel ): Future[Option[DataFragment]] = {
    val opInputs: List[OperationInput] = getOperationInputs( context )
    val optSection: Option[ma2.Section] = context.getOpSectionIntersection
    val kernelContext = context.toKernelContext
    logger.info( "mapReduce: opInputs = " + opInputs.map( df => "%s(%s)".format( df.getKeyString, df.fragmentSpec.toString ) ).mkString( "," ))
    val future_results: IndexedSeq[Future[Option[DataFragment]]] = ( 0 until nprocs ) map (
      iproc => Future { kernel.map ( iproc, opInputs map ( _.domainDataFragment( iproc, optSection ) ), kernelContext ) }
      )
    reduce(future_results, context, kernel )
  }

  def executeProcess( context: CDASExecutionContext, kernel: Kernel  ): ExecutionResult = {
    val t0 = System.nanoTime()
    var opResult: Future[Option[DataFragment]] = mapReduce( context, kernel )
    opResult.onComplete {
      case Success(dataFragOpt) =>
        logger.info(s"********** Completed Execution of Kernel[%s(%s)]: %s , total time = %.3f sec  ********** \n".format(kernel.name,kernel.id,context.operation.toString, (System.nanoTime() - t0) / 1.0E9))
      case Failure(t) =>
        logger.error(s"********** Failed Execution of Kernel[%s(%s)]: %s ********** \n".format(kernel.name,kernel.id,context.operation.toString ))
        logger.error( " ---> Cause: " + t.getCause.getMessage )
        logger.error( "\n" + t.getCause.getStackTrace.mkString("\n") + "\n" )
    }
    createResponse( opResult, context, kernel )
  }
  def postOp( future_result: Future[Option[DataFragment]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = future_result
  def reduce( future_results: IndexedSeq[Future[Option[DataFragment]]], context: CDASExecutionContext, kernel: Kernel ):  Future[Option[DataFragment]] = Future.reduce(future_results)(kernel.reduceOp(context.toKernelContext) _)

  def createResponse( resultFut: Future[Option[DataFragment]], context: CDASExecutionContext, kernel: Kernel ): ExecutionResult = {
    val var_mdata = Map[String,Attribute]()
    val async = context.request.config("async", "false").toBoolean
    val finalResultFut = resultFut.map( _.map( pre_result=> kernel.postOp( pre_result, context.toKernelContext ) ) )
    val resultId = cacheResult( finalResultFut, context, var_mdata /*, inputVar.getVariableMetadata(context.server) */ )
    if(async) {
      new AsyncExecutionResult( resultId )
    } else {
      val resultOpt: Option[DataFragment] = Await.result( finalResultFut, Duration.Inf )
      resultOpt match {
        case Some( result) =>
          new BlockingExecutionResult (context.operation.identifier, List(result.spec), context.request.targetGrid.getSubGrid (result.spec.roi), result.data, resultId )
        case None =>
          logger.error( "Operation %s returned empty result".format( context.operation.identifier ) )
          new BlockingExecutionResult (context.operation.identifier, List(), context.request.targetGrid, CDFloatArray.empty )
      }
    }
  }

  def cacheResult( resultFut: Future[Option[DataFragment]], context: CDASExecutionContext, varMetadata: Map[String,nc2.Attribute] ): Option[String] = {
    try {
      val tOptFragFut = resultFut.map( dataFragOpt => dataFragOpt.map( dataFrag => new TransientFragment( dataFrag, context.request, varMetadata ) ) )
      collectionDataCache.putResult( context.operation.rid, tOptFragFut )
      Some(context.operation.rid)
    } catch {
      case ex: Exception => logger.error( "Can't cache result: " + ex.getMessage ); None
    }
  }

}