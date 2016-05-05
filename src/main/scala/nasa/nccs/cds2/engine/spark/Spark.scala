package nasa.nccs.cds2.engine.spark

import nasa.nccs.esgf.process.TaskRequest
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

class SparkEngine

object SparkEngine {
  val logger = LoggerFactory.getLogger( classOf[SparkEngine] )

//  lazy val conf = {
//    new SparkConf(false)
//      .setMaster("local[*]")
//      .setAppName("cdas")
//      .set("spark.logConf", "true")
//  }
//
//  lazy val sc = SparkContext.getOrCreate(conf)

  def execute( request: TaskRequest, run_args: Map[String,Any] ) = {
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
  }
}

object SimpleApp {
  // Run with: spark-submit  --class "nasa.nccs.cds2.engine.SimpleApp"  --master local[4] /usr/local/web/Spark/CDS2/target/scala-2.11/cds2_2.11-1.0-SNAPSHOT.jar
  def main(args: Array[String]) {
    val sampleFile = "README.md"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(sampleFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

object SimpleRDD {
  // Run with: spark-submit  --class "nasa.nccs.cds2.engine.SimpleRDD"  --master local[4] /usr/local/web/Spark/CDS2/target/scala-2.11/cds2_2.11-1.0-SNAPSHOT.jar
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleRDD")
    val sc = new SparkContext(conf)
    val npart = 4
    val rdd = sc.makeRDD( 1 to npart, npart )
    val values = rdd.map( i => i*10 ).collect()
    println( values.mkString("[ ",", "," ]"))
  }
}


