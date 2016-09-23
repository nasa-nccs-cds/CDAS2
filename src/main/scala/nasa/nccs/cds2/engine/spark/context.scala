package nasa.nccs.cds2.engine.spark

import nasa.nccs.caching.Partition
import nasa.nccs.cdapi.cdm.{CDSVariable, PartitionedFragment}
import nasa.nccs.cdapi.data.RDDPartition
import nasa.nccs.cdapi.kernels.CDASExecutionContext
import nasa.nccs.esgf.process.{DataFragment, DomainAxis, OperationSpecs}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CDSparkContext {
  val kyro_buffer_mb = 24
  val kyro_buffer_max_mb = 64

  def apply() : CDSparkContext = new CDSparkContext( new SparkContext( getSparkConf() ) )
  def apply( conf: SparkConf ) : CDSparkContext = new CDSparkContext( new SparkContext(conf) )
  def apply( context: SparkContext ) : CDSparkContext = new CDSparkContext( context )
  def apply( url: String, name: String ) : CDSparkContext = new CDSparkContext( new SparkContext( getSparkConf(url,name) ) )

  def getSparkConf( master: String="local[*]", appName: String="CDAS", logConf: Boolean = true ) = new SparkConf(false)
    .setMaster( master )
    .setAppName( appName )
    .set("spark.logConf", logConf.toString )
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.mb",kyro_buffer_mb.toString)
    .set("spark.kryoserializer.buffer.max.mb",kyro_buffer_max_mb.toString)
}

class CDSparkContext( @transient val sparkContext: SparkContext ) {

  val logger = LoggerFactory.getLogger(this.getClass)

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

  def cacheRDDPartition( partFrag: PartitionedFragment ): RDD[RDDPartition] = {
    val nPart = partFrag.partitions.parts.length
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => partFrag.partRDDPartition( iPart ) )
  }

  def domainRDDPartition( partFrags: List[PartitionedFragment], context: CDASExecutionContext ): RDD[ RDDPartition ] = {
    val nPart = partFrags.head.partitions.parts.length                                                                                    // TODO: commensurate partitions?
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => RDDPartition.merge( partFrags.flatMap( _.domainRDDPartition( iPart, context ) ) ) )
  }

}

