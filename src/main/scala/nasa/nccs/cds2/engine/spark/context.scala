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
    logger.info( " Loggers: " + LogManager.getCurrentLoggers.map( _.asInstanceOf[Logger].getName ).mkString(",") )
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

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

  def cacheRDDPartition( partFrag: PartitionedFragment ): RDD[RDDPartition] = {
    val nPart = partFrag.partitions.parts.length
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => partFrag.partRDDPartition( iPart ) )
  }

  def getPartitions( opInputs: Iterable[OperationInput] ): Option[Partitions] = {
    for( opInput <- opInputs ) opInput match {
      case pfrag: PartitionedFragment => return Some( pfrag.partitions )
      case _ => None
    }
    None
  }

  def getRDD( uid: String, pFrag: PartitionedFragment, partitions: Partitions, opSection: Option[ma2.Section] ): RDD[RDDPartition] = {
    val rddSpecs: Array[RDDPartSpec] = partitions.parts.map(partition => RDDPartSpec(partition, List(pFrag.getRDDVariableSpec(uid, partition, opSection))))
//    log( " Create RDD, rddParts = " + rddSpecs.map(_.toXml.toString()).mkString(",") )
    sparkContext.parallelize(rddSpecs).map(_.getRDDPartition)
  }
  def getRDD( uid: String, tVar: OperationTransientInput, partitions: Partitions, opSection: Option[ma2.Section] ): RDD[RDDPartition] = {
    val rddParts = partitions.parts.indices.map( RDDPartition( _, tVar.variable.result ) )
//    log( " Create RDD, rddParts = " + rddParts.map(_.toXml.toString()).mkString(",") )
    sparkContext.parallelize(rddParts)
  }


  def domainRDDPartition( opInputs: Map[String,OperationInput], context: CDASExecutionContext): RDD[RDDPartition] = {
    val opSection: Option[ma2.Section] = context.getOpSectionIntersection
    val rdds: Iterable[RDD[RDDPartition]] = getPartitions(opInputs.values) match {
      case Some(partitions) => opInputs.map {
          case (uid: String, pFrag: PartitionedFragment) =>
            getRDD( uid, pFrag, partitions, opSection )
          case (uid: String, tVar: OperationTransientInput ) =>
            getRDD( uid, tVar, partitions, opSection )
          case (uid, x ) => throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
        }
      case None => opInputs.map {
          case (uid: String, tVar: OperationTransientInput ) => sparkContext.parallelize(List(0)).map(_ => tVar.variable.result )
          case (uid, x ) => throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
        }
    }
    if( opInputs.size == 1 ) rdds.head else rdds.tail.foldLeft( rdds.head )( CDSparkContext.merge(_,_) )
  }

}


