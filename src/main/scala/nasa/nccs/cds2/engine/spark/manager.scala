package nasa.nccs.cds2.engine.spark

import nasa.nccs.caching.{CollectionDataCacheMgr, RDDTransientVariable, collectionDataCache}
import nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.cdm.{OperationInput, PartitionedFragment}
import nasa.nccs.cdapi.data.RDDPartition
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.engine.CDS2ExecutionManager
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.cdsutils
import nasa.nccs.wps.{RDDExecutionResult, WPSExecuteResponse, WPSResponse}
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


class CDSparkExecutionManager( val cdsContext: CDSparkContext = CDSparkContext() ) extends CDS2ExecutionManager {

  def mapReduce(context: CDASExecutionContext, kernel: Kernel ): RDDPartition = {
    val kernelContext = context.toKernelContext
    val inputs = prepareInputs( context )
    val mapresult = map( inputs, kernelContext, kernel )
    reduce( mapresult, kernelContext, kernel )
  }

  def prepareInputs( context: CDASExecutionContext ): RDD[(Int,RDDPartition)] = {
    logger.info( "\n\n ----------------------- BEGIN prepare Inputs -------\n" )
    val t0 = System.nanoTime()
    val opInputs: Map[String,OperationInput] = getOperationInputs( context )
    val inputs: RDD[(Int,RDDPartition)] = cdsContext.domainRDDPartition( opInputs, context ).sortByKey(true)
    logger.info( "\n\n ----------------------- FINISHED prepare Inputs, time = %.3f sec ----------------------- ".format((System.nanoTime() - t0) / 1.0E9))
    inputs
  }

  def map( input: RDD[(Int,RDDPartition)], context: KernelContext, kernel: Kernel ): RDD[(Int,RDDPartition)] = {
    logger.info( "\n\n ----------------------- BEGIN map Operation -------\n")
    val t0 = System.nanoTime()
    val result = input.map( rdd_part => kernel.map( rdd_part, context ) )
    logger.info( "\n\n ----------------------- FINISHED map Operation, time = %.3f sec ----------------------- ".format((System.nanoTime() - t0) / 1.0E9))
    result
  }

  def executeProcess( context: CDASExecutionContext, kernel: Kernel  ): WPSExecuteResponse = {
    val t0 = System.nanoTime()
    var pre_result: RDDPartition = mapReduce( context, kernel )
    val kernelContext = context.toKernelContext
    val t1 = System.nanoTime()
    val result = kernel.postRDDOp( pre_result, kernelContext  )
    val t2 = System.nanoTime()
    logger.info(s"********** Completed Execution of Kernel[%s(%s)]: %s , total time = %.3f sec, postOp time = %.3f sec   ********** \n".format(kernel.name,kernel.id,context.operation.toString, (t2 - t0) / 1.0E9, (t2 - t1) / 1.0E9))
//    logger.info( "\n\nResult partition elements= %s \n\n".format( result.elements.values.map( cdsutils.toString(_) ) ) )
    createResponse( kernel, result, context )
  }

  def reduce( mapresult: RDD[(Int,RDDPartition)], context: KernelContext, kernel: Kernel ): RDDPartition = {
    logger.info( "\n\n ----------------------- BEGIN reduce Operation ----------------------- \n" )
    val t0 = System.nanoTime()
    val result = if( kernel.reduceCombineOpt.isDefined && context.getAxes.includes(0) ) {
      mapresult.reduce( kernel.reduceRDDOp(context) _ )._2
    } else {
      val results: Seq[(Int, RDDPartition)] =  mapresult.collect().toSeq.sortWith(_._1 < _._1)
      val t1 = System.nanoTime()
      logger.info( "REDUCE STAGES >>>===> Collect: %.3f sec".format( (t1 - t0) / 1.0E9 ))
      results.tail.foldLeft( results.head._2 )( { case (r0,(index,r1)) => kernel.mergeRDD(r0,r1) } )
    }
    logger.info( "\n\n ----------------------- FINISHED reduce Operation, time = %.3f sec ----------------------- ".format((System.nanoTime() - t0) / 1.0E9))
    result
  }

  def createResponse( kernel: Kernel, result: RDDPartition, context: CDASExecutionContext ): WPSExecuteResponse = {
    val resultId = cacheResult( result, context )
    new RDDExecutionResult( "", kernel, context.operation.identifier, result, resultId ) // TODO: serviceInstance
  }

  def cacheResult( result: RDDPartition, context: CDASExecutionContext ): Option[String] = {
    try {
      collectionDataCache.putResult( context.operation.rid, new RDDTransientVariable(result,context.operation,context.request) )
      logger.info( " ^^^^## Cached result, results = " + collectionDataCache.getResultIdList.mkString(",") )
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


