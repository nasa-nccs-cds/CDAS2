package nasa.nccs.cdas.engine.spark

import java.nio.file.{Files, Paths}

import nasa.nccs.caching._
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data._
import ucar.nc2.time.CalendarDate
import nasa.nccs.cdas.engine.WorkflowNode
import nasa.nccs.cdas.kernels.{Kernel, KernelContext}
import nasa.nccs.cdas.utilities.appParameters
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.Loggable
import org.apache.spark.rdd.RDD
import nasa.nccs.cdas.utilities
import org.apache.spark.{Partitioner, SparkConf, SparkContext, SparkEnv}
import ucar.ma2
import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean
import nasa.nccs.cdapi.tensors.CDCoordMap
import nasa.nccs.cdas.portal.TestApplication.logger
import ucar.nc2.dataset.CoordinateAxis1DTime

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CDSparkContext extends Loggable {
  val mb = 1024 * 1024
  val totalRAM = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean].getTotalPhysicalMemorySize / mb
  val runtime = Runtime.getRuntime
  val default_executor_memory = (totalRAM-10).toString + "m"
  val default_executor_cores = (runtime.availableProcessors-1).toString
  val default_num_executors = "1"

  def apply( appName: String="CDAS", logConf: Boolean = true, enableMetrics: Boolean = false ) : CDSparkContext = {
    logger.info( "--------------------------------------------------------")
    logger.info( "   ****  NEW CDSparkContext Created  **** ")
    logger.info( "--------------------------------------------------------\n\n")

    val cl = ClassLoader.getSystemClassLoader
//    logger.info( "Loaded jars: \n\t" + cl.asInstanceOf[java.net.URLClassLoader].getURLs.mkString("\n\t") )
//    logger.info( "CDAS env: \n\t" +  ( System.getenv.map { case (k,v) => k + ": " + v } ).mkString("\n\t") )

    val sparkContext = new SparkContext( getSparkConf( appName, logConf, enableMetrics) )
    sparkContext.setLogLevel( appParameters("spark.log.level", "WARN" ) )
    val rv = new CDSparkContext( sparkContext )

    logger.info( "--------------------------------------------------------")
    logger.info( "   ****  CDSparkContext Creation FINISHED  **** ")
    logger.info( "--------------------------------------------------------")
    rv
  }

  def getWorkerSignature: String = {
    val node_name = ManagementFactory.getRuntimeMXBean.getName.split("@").last.split(".").head
    val thread: Thread = Thread.currentThread()
    s"E${SparkEnv.get.executorId}:${node_name}:${thread.getName}:${thread.getId}"
  }

  def apply( conf: SparkConf ) : CDSparkContext = new CDSparkContext( new SparkContext(conf) )
  def apply( context: SparkContext ) : CDSparkContext = new CDSparkContext( context )

  def merge(rdd0: RDD[(RecordKey,RDDRecord)], rdd1: RDD[(RecordKey,RDDRecord)] ): RDD[(RecordKey,RDDRecord)] = {
    val mergedRdd = rdd0.join( rdd1 ) mapValues { case (part0,part1) => part0 ++ part1  } map identity
    if ( mergedRdd.isEmpty() ) {
      val keys0 = rdd0.keys.collect()
      val keys1 = rdd1.keys.collect()
      val msg = s"Empty merge ==> keys0: ${keys0.mkString(",")} keys1: ${keys1.mkString(",")}"
      logger.error(msg)
      throw new Exception(msg)
    }
    val result = rdd0.partitioner match { case Some(p) => mergedRdd.partitionBy(p); case None => rdd1.partitioner match { case Some(p) => mergedRdd.partitionBy(p); case None => mergedRdd } }
    result
  }

  def append(p0: (RecordKey,RDDRecord), p1: (RecordKey,RDDRecord) ): (RecordKey,RDDRecord) = ( p0._1 + p1._1, p0._2.append(p1._2) )

  def getSparkConf( appName: String, logConf: Boolean, enableMetrics: Boolean  ) = {
    val cdas_cache_dir = appParameters.getCacheDirectory
    val sc = new SparkConf(false)
      .setAppName( appName )
      .set("spark.logConf", logConf.toString )
      .set("spark.local.dir", cdas_cache_dir )
      .set("spark.file.transferTo", "false" )
      .set("spark.kryoserializer.buffer.max", "1000m" )
//      .set("spark.driver.maxResultSize", "8000m" )
      .registerKryoClasses( Array(classOf[DirectRDDRecordSpec], classOf[RecordKey], classOf[RDDRecord], classOf[DirectRDDVariableSpec], classOf[CDSection], classOf[HeapFltArray], classOf[Partition], classOf[CDCoordMap] ) )

    if( enableMetrics ) sc.set("spark.metrics.conf", getClass.getResource("/spark.metrics.properties").getPath )
    appParameters( "spark.master" ) match {
      case Some(cval) =>
        logger.info( s" >>>>> Set Spark Master from appParameters: $cval" )
        sc.setMaster(cval)
      case None => Unit
    }
    utilities.runtime.printMemoryUsage
    utilities.runtime.printMemoryUsage(logger)
    sc
  }

  def addConfig( sc: SparkConf, spark_config_id: String, cdas_config_id: String ) =  appParameters( cdas_config_id ) map ( cval => sc.set( spark_config_id, cval ) )

  def getPartitioner( rdd: RDD[(RecordKey,RDDRecord)] ): RangePartitioner = {
    rdd.partitioner match {
      case Some( partitioner ) => partitioner match {
        case range_partitioner: RangePartitioner => range_partitioner
        case wtf => throw new Exception( "Found partitioner of wrong type: " + wtf.getClass.getName )
      }
      case None =>
        throw new Exception( "Missing partitioner for rdd"  )
    }
  }

  def coalesce(rdd: RDD[(RecordKey,RDDRecord)], context: KernelContext ): RDD[(RecordKey,RDDRecord)] = {
    if ( rdd.getNumPartitions > 1 ) {
//      val partitioner: RangePartitioner = getPartitioner(rdd).colaesce
//      var repart_rdd = rdd repartitionAndSortWithinPartitions partitioner
      val partitioner: RangePartitioner = getPartitioner(rdd)
      rdd.sortByKey( true, 1 ) glom() map (_.fold((partitioner.range.startPoint, RDDRecord.empty))((x, y) => { (x._1 + y._1, x._2.append(y._2)) }))
    } else { rdd }
  }

//  def coalesce( rdd: RDD[(PartitionKey,RDDPartition)], context: KernelContext ): RDD[(PartitionKey,RDDPartition)] = {
//    if ( rdd.getNumPartitions > 1 ) {
//      val partitioner: RangePartitioner = getPartitioner(rdd)
//      rdd.sortByKey(true,1) reduceByKey Kernel.mergeRDD(context)
//      rdd.sortByKey( true, 1 ) reduce Kernel.mergeRDD(context)
//    } else { rdd }
//  }

  def splitPartition(key: RecordKey, part: RDDRecord, partitioner: RangePartitioner ): IndexedSeq[(RecordKey,RDDRecord)] = {
    logger.info( s"CDSparkContext.splitPartition: KEY-${key.toString} -> PART-${part.getShape.mkString(",")}")
    val rv = partitioner.intersect(key) map ( partkey =>
      (partkey -> part.slice(partkey.elemStart, partkey.numElems)) )
    rv
  }

  def applyPartitioner( partitioner: RangePartitioner )( elems: Iterator[(RecordKey,RDDRecord)] ): Iterator[(RecordKey,RDDRecord)] =
    elems.map { case (key,part ) => splitPartition(key,part,partitioner) } reduce ( _ ++ _ ) toIterator

  def repartition(rdd: RDD[(RecordKey,RDDRecord)], partitioner: RangePartitioner ): RDD[(RecordKey,RDDRecord)] =
    rdd.mapPartitions( applyPartitioner(partitioner), true ) repartitionAndSortWithinPartitions partitioner reduceByKey(_ ++ _)

}

class CDSparkContext( @transient val sparkContext: SparkContext ) extends Loggable {
  implicit val strRep: LongRange.StrRep = CalendarDate.of(_).toString

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def totalClusterCores: Int = sparkContext.defaultParallelism

  def getConf: SparkConf = sparkContext.getConf

  def cacheRDDPartition( partFrag: PartitionedFragment, startTime : Long ): RDD[RDDRecord] = {
    val nPart = partFrag.partitions.parts.length
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => partFrag.partRDDPartition( iPart, startTime ) )
  }

  def getPartitions( opInputs: Iterable[OperationInput] ): Option[CachePartitions] = {
    for( opInput <- opInputs ) opInput match {
      case pfrag: PartitionedFragment => return Some( pfrag.partitions )
      case _ => None
    }
    None
  }

  def parallelize(partition: RDDRecord, partitioner: RangePartitioner ): RDD[(RecordKey,RDDRecord)] = {
    val new_partitions = partitioner.partitions.values.map( partKey => partKey -> partition.slice( partKey.elemStart, partKey.numElems ) )
    sparkContext parallelize new_partitions.toSeq repartitionAndSortWithinPartitions partitioner
  }

  def getRDD( uid: String, pFrag: PartitionedFragment, requestCx: RequestContext, opSection: Option[ma2.Section], node: WorkflowNode, batchIndex: Int, kernelContext: KernelContext ): Option[ RDD[(RecordKey,RDDRecord)] ] = {
    val partitions = pFrag.partitions
    val tgrid: TargetGrid = pFrag.getGrid
    val rddPartSpecs: Array[RDDPartSpec] = partitions.getBatch(batchIndex) map (partition =>
      RDDPartSpec(partition, tgrid, List(pFrag.getRDDVariableSpec(uid, partition, opSection)))
      ) filterNot (_.empty(uid))
    logger.info("Discarded empty partitions: Creating RDD with <<%d>> items".format( rddPartSpecs.length ))
    if (rddPartSpecs.length == 0) { None }
    else {
      val partitioner = RangePartitioner( rddPartSpecs.map(_.timeRange))
      val parallelized_rddspecs = sparkContext parallelize rddPartSpecs keyBy (_.timeRange) partitionBy partitioner
      Some( parallelized_rddspecs mapValues (spec => spec.getRDDPartition(kernelContext,batchIndex)) )     // repartitionAndSortWithinPartitions partitioner
    }
  }

  def getRDD(uid: String, directInput: CDASDirectDataInput, requestCx: RequestContext, opSection: Option[ma2.Section], node: WorkflowNode, batchIndex: Int, kernelContext: KernelContext ): Option[RDD[(RecordKey,RDDRecord)]] = {
    directInput.getPartitioner(opSection) flatMap ( partMgr => {
      val partitions = partMgr.partitions
      val tgrid: TargetGrid = requestCx.getTargetGrid(uid).getOrElse(throw new Exception("Missing target grid for uid " + uid))
      val batch= partitions.getBatch(batchIndex)
      val rddPartSpecs: Array[DirectRDDPartSpec] = batch map ( partition => DirectRDDPartSpec(partition, tgrid, List(directInput.getRDDVariableSpec(uid, opSection)))) filterNot (_.empty(uid))
      if (rddPartSpecs.length == 0) { None }
      else {
        logger.info("\n **************************************************************** \n ---> Processing Batch %d: Creating input RDD with <<%d>> partitions for node %s".format(batchIndex,rddPartSpecs.length,node.getNodeId))
        val partitioner = RangePartitioner( rddPartSpecs.map(_.timeRange) )
        logger.info("Creating RDD with records:\n\t" + rddPartSpecs.flatMap( _.getRDDRecordSpecs() ).map( _.toString ).mkString("\n\t"))
        val parallelized_rddspecs = sparkContext parallelize rddPartSpecs.flatMap( _.getRDDRecordSpecs() ) keyBy (_.timeRange) partitionBy partitioner
        Some(parallelized_rddspecs mapValues (spec => spec.getRDDPartition(kernelContext,batchIndex)) )
      }
    } )
  }

  def getRDD(uid: String, extInput: ExternalDataInput, requestCx: RequestContext, opSection: Option[ma2.Section], node: WorkflowNode, kernelContext: KernelContext, batchIndex: Int ): Option[ RDD[ (RecordKey,RDDRecord) ] ] = {
    val tgrid: TargetGrid = extInput.getGrid
    val ( key, varSpec ) = extInput.getKeyedRDDVariableSpec(uid, opSection)
    val rddPartSpec = ExtRDDPartSpec( key, List(varSpec) )
    val parallelized_rddspecs = sparkContext parallelize Seq( rddPartSpec ) keyBy (_.timeRange)
    Some( parallelized_rddspecs mapValues ( spec => spec.getRDDPartition(kernelContext,batchIndex) ) )
  }

 /* def inputConversion( dataInput: PartitionedFragment, targetGrid: TargetGrid ): PartitionedFragment = {
    dataInput.fragmentSpec.targetGridOpt match {
      case Some(inputTargetGrid) =>
        val inputTimeAxis: CoordinateAxis1DTime = inputTargetGrid.grid.getAxisSpec( 0 ).getTimeAxis
        val timeAxis: CoordinateAxis1DTime = targetGrid.grid.getAxisSpec( 0 ).getTimeAxis
        if( inputTimeAxis.equals(timeAxis) ) dataInput
        else dataInput.timeConversion( timeAxis )
      case None =>
        throw new Exception("Missing target grid for fragSpec: " + dataInput.fragmentSpec.toString)
    }
  }*/

//  def getRDD( uid: String, tVar: OperationTransientInput, tgrid: TargetGrid, partitions: Partitions, opSection: Option[ma2.Section] ): RDD[(PartitionKey,RDDPartition)] = {
//    val rddParts: IndexedSeq[(PartitionKey,RDDPartition)] = partitions.parts map { case part => ( part.getPartitionKey(tgrid) -> RDDPartition( tVar.variable.result ) ) }
////    log( " Create RDD, rddParts = " + rddParts.map(_.toXml.toString()).mkString(",") )
//    logger.info( "Creating Transient RDD with <<%d>> paritions".format( rddParts.length ) )
//    sparkContext.parallelize(rddParts)
//  }


/*
  def domainRDDPartition( opInputs: Map[String,OperationInput], context: CDASExecutionContext): RDD[(Int,RDDPartition)] = {
    val opSection: Option[ma2.Section] = context.getOpSectionIntersection
    val rdds: Iterable[RDD[(Int,RDDPartition)]] = getPartitions(opInputs.values) match {
      case Some(partitions) => opInputs.map {
          case (uid: String, pFrag: PartitionedFragment) =>
            getRDD( uid, pFrag, partitions, opSection )
          case (uid: String, tVar: OperationTransientInput ) =>
            getRDD( uid, tVar, partitions, opSection )
          case (uid, x ) => throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
        }
      case None => opInputs.map {
          case (uid: String, tVar: OperationTransientInput ) => sparkContext.parallelize(List(0)).map(index => index -> tVar.variable.result )
          case (uid, x ) => throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
        }
    }
    if( opInputs.size == 1 ) rdds.head else rdds.tail.foldLeft( rdds.head )( CDSparkContext.merge(_,_) )
  }
  */

}


