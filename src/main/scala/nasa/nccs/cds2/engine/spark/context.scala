package nasa.nccs.cds2.engine.spark

import nasa.nccs.caching.Partition
import nasa.nccs.cdapi.cdm.{CDSVariable, PartitionedFragment}
import nasa.nccs.cdapi.kernels.CDASExecutionContext
import nasa.nccs.esgf.process.{DataFragment, DomainAxis, OperationSpecs}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CDSparkContext {
  def apply() : CDSparkContext = {
    val conf =  new SparkConf(false).setMaster("local[*]").setAppName("CDAS").set("spark.logConf", "true")
    new CDSparkContext( new SparkContext( conf ) )
  }
  def apply( conf: SparkConf ) : CDSparkContext = new CDSparkContext( new SparkContext(conf) )
  def apply( context: SparkContext ) : CDSparkContext = new CDSparkContext( context )
  def apply( url: String, name: String ) : CDSparkContext = new CDSparkContext( new SparkContext(  new SparkConf().setMaster(url).setAppName(name) ) )
}

object CDSparkPartition {
  def apply( iPartIndex: Int, dataFragments: List[Option[DataFragment]] ) = new  CDSparkPartition( iPartIndex, dataFragments )
}

class CDSparkPartition( val iPartIndex: Int, val dataFragments: List[Option[DataFragment]] ) extends Serializable {}

class CDSparkContext( @transient val sparkContext: SparkContext ) {

  val logger = LoggerFactory.getLogger(this.getClass)

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

  def cacheFragmentRDD( partFrag: PartitionedFragment ): RDD[DataFragment] = {
    val nPart = partFrag.partitions.parts.length
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => partFrag.partDataFragment( iPart ) )
  }

  def domainFragmentRDD( partFrags: List[PartitionedFragment], context: CDASExecutionContext ): RDD[ CDSparkPartition ] = {
    val nPart = partFrags.head.partitions.parts.length                                                                                    // TODO: commensurate partitions?
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => CDSparkPartition( iPart, partFrags.map( _.domainDataFragment( iPart, context ) ) ) )
  }

}

