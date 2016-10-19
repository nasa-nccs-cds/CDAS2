package nasa.nccs.cds2.engine.spark

import java.nio.file.Paths

import nasa.nccs.caching.{CDASPartitioner, Partition, Partitions}
import nasa.nccs.cdapi.cdm.{CDSVariable, OperationInput, OperationTransientInput, PartitionedFragment}
import nasa.nccs.cdapi.data.{HeapFltArray, RDDPartSpec, RDDPartition, RDDVariableSpec}
import nasa.nccs.cdapi.kernels.CDASExecutionContext
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.utilities.appParameters
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.Loggable
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ucar.ma2
import org.apache.log4j.{ Logger, LogManager, Level }
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CDSparkContext extends Loggable {
  val kyro_buffer_mb = 24
  val kyro_buffer_max_mb = 64
  val default_master = "local[%d]".format(CDASPartitioner.nProcessors)

  def apply( master: String=default_master, appName: String="CDAS", logConf: Boolean = false ) : CDSparkContext = {
    logger.info( "--------------------------------------------------------")
    logger.info( "   ****  NEW CDSparkContext Created  **** ")
    logger.info( "--------------------------------------------------------\n\n")

    val sparkContext = new SparkContext( getSparkConf(master, appName, logConf) )
    sparkContext.setLogLevel("WARN")
    val rv = new CDSparkContext( sparkContext )

    logger.info( "--------------------------------------------------------")
    logger.info( "   ****  CDSparkContext Creation FINISHED  **** ")
    logger.info( "--------------------------------------------------------")
    logger.info( "\n\n LOGGERS:  >>>>------------> " + LogManager.getCurrentLoggers.toList.map( _.asInstanceOf[Logger] ).map( logger => logger.getName + " -> " + logger.getLevel.toString ).mkString(",") )
    rv
  }

  def apply( conf: SparkConf ) : CDSparkContext = new CDSparkContext( new SparkContext(conf) )
  def apply( context: SparkContext ) : CDSparkContext = new CDSparkContext( context )
  def apply( url: String, name: String ) : CDSparkContext = new CDSparkContext( new SparkContext( getSparkConf( url, name, false ) ) )

  def merge( rdd0: RDD[RDDPartition], rdd1: RDD[RDDPartition] ): RDD[RDDPartition] = rdd0.zip(rdd1).map( p => p._1 ++ p._2 )

  def getSparkConf( master: String, appName: String, logConf: Boolean  ) = new SparkConf(false)
    .setMaster( master )
    .setAppName( appName )
    .set("spark.logConf", logConf.toString )
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //
    .set("spark.kryoserializer.buffer",kyro_buffer_mb.toString)
    .set("spark.kryoserializer.buffer.max",kyro_buffer_max_mb.toString)
}

class CDSparkContext( @transient val sparkContext: SparkContext ) extends Loggable {
  val logWriter = new java.io.PrintWriter( Paths.get( appParameters.cacheDir, "cdas.spark.log" ).toFile )

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def log( msg: String ) = logWriter.write( msg + "\n" )
  def log( partIndex: Int, msg: String ) = logWriter.write( "P[%d]: %s\n".format( partIndex, msg ) )

  def getConf: SparkConf = sparkContext.getConf

  def cacheRDDPartition( partFrag: PartitionedFragment ): RDD[RDDPartition] = {
    val nPart = partFrag.partitions.parts.length
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => partFrag.partRDDPartition( iPart ) )
  }

  def getPartitions( opInputs: List[OperationInput] ): Option[Partitions] = {
    for( opInput <- opInputs ) opInput match {
      case pfrag: PartitionedFragment => return Some( pfrag.partitions )
      case _ => None
    }
    None
  }

  def getRDD( pFrag: PartitionedFragment, partitions: Partitions, opSection: Option[ma2.Section] ): RDD[RDDPartition] = {
    val rddSpecs: Array[RDDPartSpec] = partitions.parts.map(partition => RDDPartSpec(partition, List(pFrag.getRDDVariableSpec(partition, opSection))))
//    log( " Create RDD, rddParts = " + rddSpecs.map(_.toXml.toString()).mkString(",") )
    sparkContext.parallelize(rddSpecs).map(_.getRDDPartition)
  }
  def getRDD( tVar: OperationTransientInput, partitions: Partitions, opSection: Option[ma2.Section] ): RDD[RDDPartition] = {
    val rddParts = partitions.parts.indices.map( RDDPartition( _, tVar.variable.result ) )
//    log( " Create RDD, rddParts = " + rddParts.map(_.toXml.toString()).mkString(",") )
    sparkContext.parallelize(rddParts)
  }


  def domainRDDPartition( opInputs: List[OperationInput], context: CDASExecutionContext): RDD[RDDPartition] = {
    val opSection: Option[ma2.Section] = context.getOpSectionIntersection
    val rdds: List[RDD[RDDPartition]] = getPartitions(opInputs) match {
      case Some(partitions) =>
        opInputs.map( opInput => opInput match {
          case pFrag: PartitionedFragment => getRDD( pFrag, partitions, opSection )
          case tVar: OperationTransientInput => getRDD( tVar, partitions, opSection )
          case _ => throw new Exception( "Unsupported OperationInput class: " + opInput.getClass.getName )
        })
      case None =>
        opInputs.map( opInput => opInput match {
          case tVar: OperationTransientInput => sparkContext.parallelize(List(0)).map(_ => tVar.variable.result )
          case _ => throw new Exception( "Unsupported OperationInput class: " + opInput.getClass.getName )
        })
    }
    if( opInputs.length == 1 ) rdds.head else rdds.tail.foldLeft( rdds.head )( CDSparkContext.merge(_,_) )
  }

}


