package nasa.nccs.cds2.engine.spark

import nasa.nccs.cdapi.cdm.{PartitionedFragment, CDSVariable}
import nasa.nccs.esgf.process.{OperationSpecs, DomainAxis}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


class CDSparkContext(val conf: SparkConf) {

  val logger = LoggerFactory.getLogger(this.getClass)
  val sparkContext = new SparkContext(conf)

  def this(url: String, name: String) {
    this(new SparkConf().setMaster(url).setAppName(name))
  }

  def this(url: String, name: String, parser: (String) => (String)) {
    this(new SparkConf().setMaster(url).setAppName(name))
  }

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

//  def makeFragmentRDD( variable: CDSVariable, roi: List[DomainAxis], partAxis: Char, nPart: Int, axisConf: List[OperationSpecs] ): RDD[PartitionedFragment] = {
//    val indexRDD: RDD[Int] = sparkContext.makeRDD( 0 to nPart-1, nPart )
//    indexRDD.map( variable.loadRoiPartition( roi, _, partAxis, nPart ) )
//  }
}

