package nasa.nccs.cds2.engine.spark

import nasa.nccs.caching.Partition
import nasa.nccs.cdapi.cdm.{CDSVariable, PartitionedFragment}
import nasa.nccs.cdapi.kernels.CDASExecutionContext
import nasa.nccs.esgf.process.{DataFragment, DomainAxis, OperationSpecs}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CDSparkContext {
  def apply( conf: SparkConf ) : CDSparkContext = new CDSparkContext( new SparkContext(conf) )
  def apply( context: SparkContext ) : CDSparkContext = new CDSparkContext( context )
  def apply( url: String, name: String ) : CDSparkContext = new CDSparkContext( new SparkContext(  new SparkConf().setMaster(url).setAppName(name) ) )
}

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

  def domainFragmentRDD( partFrag: PartitionedFragment, context: CDASExecutionContext ): RDD[ Option[DataFragment] ] = {
    val nPart = partFrag.partitions.parts.length
    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
    indexRDD.map( iPart => partFrag.domainDataFragment( iPart, context ) )
  }

}

