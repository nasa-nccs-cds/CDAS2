package nasa.nccs.cds2.engine.spark

import nasa.nccs.caching.{CollectionDataCacheMgr, collectionDataCache}
import nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.cdm.{OperationInput, PartitionedFragment}
import nasa.nccs.cdapi.data.RDDPartition
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

  def mapReduce(context: CDASExecutionContext, kernel: Kernel ): Option[DataFragment] = {
    val opInputs: List[PartitionedFragment] = getOperationInputs( context ).flatMap(  _ match { case pf: PartitionedFragment => Some(pf); case x => None } )   // TODO: Ignores Transient Fragments
    logger.info( "mapReduce: opInputs = " + opInputs.map( df => "%s(%s)".format( df.getKeyString, df.fragmentSpec.toString ) ).mkString( "," ))
    val inputRDD: RDD[ RDDPartition ] = cdsContext.domainRDDPartition( opInputs, context )
    val mapresult: RDD[Option[RDDPartition]] = inputRDD.map( rdd_part => kernel.map( rdd_part, context ) )
    reduce( mapresult, context, kernel )
  }

  def executeProcess( context: CDASExecutionContext, kernel: Kernel  ): ExecutionResult = {
    val t0 = System.nanoTime()
    var pre_result: Option[DataFragment] = mapReduce( context, kernel )
    logger.info(s"********** Completed Execution of Kernel[%s(%s)]: %s , total time = %.3f sec  ********** \n".format(kernel.name,kernel.id,context.operation.toString, (System.nanoTime() - t0) / 1.0E9))
    createResponse( postOp( pre_result, context  ), context )
  }

  def postOp( pre_result: Option[DataFragment], context: CDASExecutionContext ):  Option[DataFragment] = pre_result
  def reduce( mapresult: RDD[Option[DataFragment]], context: CDASExecutionContext, kernel: Kernel ):  Option[DataFragment] = mapresult.reduce( kernel.reduceOp(context) _ )

  def createResponse( result: Option[DataFragment], context: CDASExecutionContext ): ExecutionResult = {    // TODO: Implement async
    val var_mdata = Map[String,Attribute]()
//    val async = context.request.config("async", "false").toBoolean
    val resultId = cacheResult( Future(result), context, var_mdata /*, inputVar.getVariableMetadata(context.server) */ )
    result match {
      case Some(result) =>
        new BlockingExecutionResult(context.operation.identifier, List(result.spec), context.request.targetGrid.getSubGrid(result.spec.roi), result.data, resultId)
      case None =>
        logger.error("Operation %s returned empty result".format(context.operation.identifier))
        new BlockingExecutionResult(context.operation.identifier, List(), context.request.targetGrid, CDFloatArray.empty)
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


