package nasa.nccs.cdas.engine.spark

import nasa.nccs.caching.{CollectionDataCacheMgr}

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


