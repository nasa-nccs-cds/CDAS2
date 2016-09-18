package nasa.nccs.cds2.engine.spark

import nasa.nccs.caching.Partition
import nasa.nccs.cdapi.cdm.{CDSVariable, PartitionedFragment}
import nasa.nccs.cdapi.kernels.CDASExecutionContext
import nasa.nccs.esgf.process.{DataFragment, DomainAxis, OperationSpecs}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


class CDSparkContext( val conf: SparkConf ) {

  val logger = LoggerFactory.getLogger(this.getClass)
  @transient val sparkContext = new SparkContext(conf)

  def this(url: String, name: String) {
    this( new SparkConf().setMaster(url).setAppName(name) )
  }

  def this(url: String, name: String, parser: (String) => (String)) {
    this( new SparkConf().setMaster(url).setAppName(name) )
  }

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

