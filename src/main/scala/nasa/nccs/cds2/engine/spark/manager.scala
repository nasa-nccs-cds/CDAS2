package nasa.nccs.cds2.engine.spark

import nasa.nccs.caching.{CollectionDataCacheMgr, collectionDataCache}
import nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.cdm.{OperationInput, PartitionedFragment}
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.engine.{CDS2ExecutionManager, SampleTaskRequests}
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.cdsutils
import org.apache.spark.rdd.RDD
import ucar.nc2
import ucar.nc2.Attribute

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

object collectionRDDDataCache extends CollectionDataCacheMgr()

//class RDDataManager( dataLoader: DataLoader ) extends ServerContext( dataLoader ) {
//  val collectionRDDataManager = new RDDataManager(collectionRDDDataCache)
//  var prdds = mutable.Map[String, RDD[PartitionedFragment]]()
//}
//  def loadRDData(cdsContext: CDSparkContext, data_container: DataContainer, domain_container: DomainContainer, nPart: Int): RDD[PartitionedFragment] = {
//    val uid: String = data_container.uid
//    val data_source: DataSource = data_container.getSource
//    val axisConf: List[OperationSpecs] = data_container.getOpSpecs
//    prdds.get(uid) match {
//      case Some(prdd) => prdd
//      case None =>
//        val dataset: cdm.CDSDataset = dataLoader.getDataset(data_source.collection,data_source.name )
//        val variable = cdsutils.time(logger, "Load Variable " + uid)(dataset.loadVariable(data_source.name))
//        val partAxis = 't' // TODO: Compute this
//      val pRDD = cdsContext.makeFragmentRDD(variable, domain_container.axes, partAxis, nPart, axisConf)
//        prdds += uid -> pRDD
//        logger.info("Loaded variable %s (%s:%s) subset data, shape = %s ".format(uid, data_source.collection, data_source.name, "")) // pRDD.shape.toString) )
//        pRDD
//    }
//  }


class CDSparkExecutionManager( val cdsContext: CDSparkContext, serverConfig: Map[String,String] = Map.empty ) extends CDS2ExecutionManager(serverConfig) {

  def mapReduce(context: CDASExecutionContext, kernel: Kernel ): Future[Option[DataFragment]] = {
    val opInputs: List[OperationInput] = getOperationInputs( context )
    logger.info( "mapReduce: opInputs = " + opInputs.map( df => "%s(%s)".format( df.getKeyString, df.fragmentSpec.toString ) ).mkString( "," ))
    val future_results: IndexedSeq[Future[Option[DataFragment]]] = ( 0 until nprocs ) map (
      iproc => Future { kernel.map ( iproc, opInputs map ( _.domainDataFragment( iproc, context ) ), context ) }
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
    createResponse( postOp( opResult, context  ), context )
  }
  def postOp( future_result: Future[Option[DataFragment]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = future_result
  def reduce( future_results: IndexedSeq[Future[Option[DataFragment]]], context: CDASExecutionContext, kernel: Kernel ):  Future[Option[DataFragment]] = Future.reduce(future_results)(kernel.reduceOp(context) _)

  def createResponse( resultFut: Future[Option[DataFragment]], context: CDASExecutionContext ): ExecutionResult = {
    val var_mdata = Map[String,Attribute]()
    val async = context.request.config("async", "false").toBoolean
    val resultId = cacheResult( resultFut, context, var_mdata /*, inputVar.getVariableMetadata(context.server) */ )
    if(async) {
      new AsyncExecutionResult( resultId )
    } else {
      val resultOpt: Option[DataFragment] = Await.result( resultFut, Duration.Inf )
      resultOpt match {
        case Some(result) =>
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
//
//  override def execute( request: TaskRequest, run_args: Map[String,String] ): xml.Elem = {
//    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
//    val data_manager = new RDDataManager( cdsContext, request.domainMap )
//    val nPart = 4  // TODO: Compute this
//    for( data_container <- request.variableMap.values; if data_container.isSource )  data_manager.loadRDData( data_container, nPart )
//    executeWorkflows( request.workflows, data_manager, run_args ).toXml
//  }
//}
//
//object sparkExecutionTest extends App {
//  import org.apache.spark.{SparkContext, SparkConf}
//  // Run with: spark-submit  --class "nasa.nccs.cds2.engine.sparkExecutionTest"  --master local[4] /usr/local/web/Spark/CDS2/target/scala-2.11/cds2_2.11-1.0-SNAPSHOT.jar
//  val conf = new SparkConf().setAppName("SparkExecutionTest")
//  val sc = new CDSparkContext(conf)
//  val npart = 4
//  val request = SampleTaskRequests.getAveArray
//  val run_args = Map[String,String]()
//  val cdsExecutionManager = new CDSparkExecutionManager( sc )
//  val result = cdsExecutionManager.execute( request, run_args )
//  println( result.toString )
//}


