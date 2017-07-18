import java.net.URI
import java.nio.file.{Path, Paths}

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdas.engine.spark.CDSparkContext
import nasa.nccs.cdas.loaders.Collections
import nasa.nccs.cdas.utilities.runtime
import nasa.nccs.utilities.{CDASLogManager, Loggable}

import scala.xml
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}
import ucar.nc2.Variable

import scala.collection.mutable.ListBuffer

class CurrentTestSuite extends FunSuite with Loggable with BeforeAndAfter {
  CDASLogManager.testing
  import nasa.nccs.cdapi.tensors.CDFloatArray
  import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
  import ucar.nc2.dataset.NetcdfDataset
  val serverConfiguration = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )
  val scontext: CDSparkContext = webProcessManager.apiManager.providers.head._2.cds2ExecutionManager.serverContext.spark
  val nExp = 3
  val shutdown_after = false
  val use_6hr_data = false
  val use_npana_data = false
  val mod_collections = for (model <- List( "GISS", "GISS-E2-R" ); iExp <- (1 to nExp)) yield (model -> s"${model}_r${iExp}i1p1")
  val cip_collections = for ( model <- List( "CIP_CFSR_6hr", "CIP_MERRA2_mon" ) ) yield (model -> s"${model}_ta")
  val eps = 0.00001
  val service = "cds2"
  val run_args = Map("async" -> "false")
  val printer = new scala.xml.PrettyPrinter(200, 3)
  val test_data_dir = sys.env.get("CDAS_HOME_DIR") match {
    case Some(cdas_home) => Paths.get( cdas_home, "src", "test", "resources", "data" )
    case None => Paths.get("")
  }
  after {
    if(shutdown_after) { cleanup() }
  }

  test("RemoveCollections") {
    Collections.removeCollections(mod_collections.map(_._2).toArray)
    for( (model, collection) <- mod_collections ) Collections.findCollection( collection ) match {
      case Some(c) => fail( "Error, failed to delete collection " + collection )
      case None => Unit
    }
    logger.info( "Successfully deleted all test collections" )
  }

  test("Aggregate") {
    for( (model, collection) <- mod_collections ) {
      val location = s"/collections/${model}/$collection.csv"
      Option(getClass.getResource(location)) match {
        case Some( url ) =>
          val collection_path = url.getFile
          val datainputs = s"""[variable=[{"uri":"collection:/$collection","path":"$collection_path"}]]"""
          val agg_result_node = executeTest (datainputs, Map.empty, "util.agg")
          logger.info (s"Agg collection $collection Result: " + printer.format (agg_result_node) )
        case None => throw new Exception( s"Can't find collection $collection for model  $model")
      }
    }
  }

  test("Aggregate1") {
    for( (model, collection) <- cip_collections ) {
      val location = s"/collections/${model}/$collection.csv"
      Option(getClass.getResource(location)) match {
        case Some( url ) =>
          val collection_path = url.getFile
          val datainputs = s"""[variable=[{"uri":"collection:/$collection","path":"$collection_path"}]]"""
          val agg_result_node = executeTest (datainputs, Map.empty, "util.agg")
          logger.info (s"Agg collection $collection Result: " + printer.format (agg_result_node) )
        case None => throw new Exception( s"Can't find collection $collection for model  $model")
      }
    }

  }

//  test("MultiAggregate") {
//    val collection_path = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/"
//    val datainputs = s"""[variable=[{"uri":"collection:/MERRA_DAILY_TEST","path":"$collection_path"}]]"""
//    val agg_result_node = executeTest (datainputs, false, "util.agg")
//    logger.info (s"Agg collection MERRA_DAILY_TEST Result: " + printer.format (agg_result_node) )
//  }


  test("Cache") {
    for( (model, collection) <- mod_collections ) {
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":150,"system":"indices"}}],variable=[{"uri":"collection:/$collection","name":"tas:v1","domain":"d0"}]]"""
      print( s"Caching collection $collection" )
      val cache_result_node = executeTest(datainputs, Map.empty, "util.cache")
      logger.info(s"Cache $collection:tas Result: " + printer.format(cache_result_node))
      runtime.printMemoryUsage
    }
    if( use_6hr_data )
      for( (model, collection) <- cip_collections ) {
        val datainputs = s"""[domain=[{"name":"d0","lat":{"start":0,"end":40,"system":"values"},"lon":{"start":0,"end":40,"system":"values"},"time":{"start":"2000-01-01","end":"2005-01-01","system":"values"}}],variable=[{"uri":"collection:/$collection","name":"ta:v1","domain":"d0"}]]"""
        print( s"Caching collection $collection" )
        val cache_result_node = executeTest(datainputs, Map.empty, "util.cache")
        logger.info(s"Cache $collection:tas Result: " + printer.format(cache_result_node))
        runtime.printMemoryUsage
      }
  }

  test("TimeConvertedDiff")  { if( use_6hr_data ) {
    print( s"Running test TimeConvertedDiff" )
    val CFSR_6hr_variable = s"""{"uri":"collection:/CIP_CFSR_6hr_ta","name":"ta:v0","domain":"d0"}"""
    val MERRA2_mon_variable = s"""{"uri":"collection:/CIP_MERRA2_mon_ta","name":"ta:v1","domain":"d0"}"""
    val datainputs = s"""[variable=[$CFSR_6hr_variable,$MERRA2_mon_variable],domain=[{"name":"d0","lat":{"start":0,"end":30,"system":"values"},"time":{"start":"2000-01-01T00:00:00Z","end":"2009-12-31T00:00:00Z","system":"values"},"lon":{"start":0,"end":30,"system":"values"}},{"name":"d1","crs":"~v1","trs":"~v0"}],operation=[{"name":"CDSpark.diff2","input":"v0,v1","domain":"d1"}]]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ).slice(0,0,10) )
    println( " ** Op Result:       " + result_data.mkDataString(", ") )
  }}

  test("subsetTestXY") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 241.2655, 241.2655, 241.2655, 241.2655, 241.2655, 241.2655, 245.2, 244.904, 244.6914, 244.5297, 244.2834, 244.0234, 245.4426, 245.1731, 244.9478, 244.6251, 244.2375, 244.0953, 248.4837, 247.4268, 246.4957, 245.586, 245.4244, 244.8213, 249.7772, 248.7458, 247.5331, 246.8871, 246.0183, 245.8848, 248.257, 247.3562, 246.3798, 245.3962, 244.6091, 243.6039 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":0,"end":5,"system":"indices"},"lon":{"start":0,"end":5,"system":"indices"},"time":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
    println( " ** NCO Result:       " + nco_verified_result.mkDataString(", ") )
    assert( getResultData( result_node ).maxScaledDiff( nco_verified_result ) < eps, s" Incorrect value computed for Sum")
  }

  test("getCapabilities") {
    val response = getCapabilities("op")
    print( response.toString )
  }

  test("getCollections") {
    val response = getCapabilities("coll")
    print( response.toString )
  }

//  test("pyZADemo") {
//    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"file:///Users/tpmaxwel/Dropbox/Tom/Data/MERRA/atmos_ua.nc","name":"ua:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.avew","input":"v1","axes":"xt","filter":"DJF"}]]"""
//    val result_node = executeTest(datainputs)
//    val result_data = CDFloatArray( getResultData( result_node ) )
//    val array_data = result_data.getArrayData(50)
//    assert( array_data.length > 0 )
//    println( " ** CDMS Result:       "  + array_data.mkString(", ") )
//  }

   test("pyWeightedAveTest") {
    val nco_result: CDFloatArray = CDFloatArray( Array( 286.2326, 286.5537, 287.2408, 288.1576, 288.9455, 289.5202, 289.6924, 289.5549, 288.8497, 287.8196, 286.8923 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.avew","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
    println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
  }

  test("pyTimeAveTestLocal") {
//    datafile=".../MERRA2_200.inst6_3d_ana_Np_T.20000101.nc4"
//    ncwa -O -v T -d lat,10,10 -d lon,20,20 -a time ${datafile} ~/test/out/time_ave.nc
//    ncdump ~/test/out/time_ave.nc
    val data_file: URI = Paths.get( test_data_dir.toString, "MERRA2_200.inst6_3d_ana_Np_T.20000101.nc4" ).toUri
    val nco_result: CDFloatArray = CDFloatArray( Array(  Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN,  262.6826, 261.1128, 259.5385, 257.9672, 256.5204, 254.8353, 253.2784, 251.6964, 247.9638, 243.8583, 239.538, 235.9563, 232.1338, 227.2614, 221.6774, 216.0401 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":10,"end":10,"system":"indices"},"lon":{"start":20,"end":20,"system":"indices"}}],variable=[{"uri":"%s","name":"T:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.ave","input":"v1","domain":"d0","axes":"t"}]]""".format( data_file.toString )
    val result_node = executeTest( datainputs, Map("numParts"->"2") )
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkBoundedDataString(", ",21) )
    println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
    assert( result_data.sample(21).maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
  }

  test("timeAveTestLocal") {
    //    datafile=".../MERRA2_200.inst6_3d_ana_Np_T.20000101.nc4"
    //    ncwa -O -v T -d lat,10,10 -d lon,20,20 -a time ${datafile} ~/test/out/time_ave.nc
    //    ncdump ~/test/out/time_ave.nc
    val data_file: URI = Paths.get( test_data_dir.toString, "MERRA2_200.inst6_3d_ana_Np_T.20000101.nc4" ).toUri
    val nco_result: CDFloatArray = CDFloatArray( Array(   9.9999999E14, 9.9999999E14, 9.9999999E14, 9.9999999E14, 9.9999999E14, 262.6826, 261.1128, 259.5385, 257.9672, 256.5204, 254.8353, 253.2784, 251.6964, 247.9638, 243.8583, 239.538, 235.9563, 232.1338, 227.2614, 221.6774, 216.0401 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":10,"end":10,"system":"indices"},"lon":{"start":20,"end":20,"system":"indices"}}],variable=[{"uri":"%s","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]""".format( data_file.toString )
    val result_node = executeTest( datainputs, Map("numParts"->"2") )
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkBoundedDataString(", ",16) )
    println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
    assert( result_data.sample(21).maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
  }

  test("NCML-timeAveTestLocal") {
    val data_file = "file:///Users/tpmaxwel/.cdas/cache/collections/NCML/MERRA2-6hr-ana_Np.200001.ncml"
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":10,"end":10,"system":"indices"},"lon":{"start":20,"end":20,"system":"indices"}}],variable=[{"uri":"%s","name":"T:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]""".format( data_file )
    val result_node = executeTest( datainputs, Map("numParts"->"2") )
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkBoundedDataString(", ",16) )
  }

  test("NCML-timeBinAveTestLocal") {
    val data_file = "http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/CFSR/6hr/atmos/ta_2000s.ncml"
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":180,"end":180,"system":"indices"},"lon":{"start":20,"end":20,"system":"indices"},"level":{"start":20,"end":20,"system":"indices"}}],variable=[{"uri":"%s","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","cycle":"diurnal","bin":"month","axes":"t"}]]""".format( data_file )
    val result_node = executeTest( datainputs, Map( "numParts" -> "4" ) )
    val result_data = getResultDataArraySeq( result_node )
    println( " ** CDMS Results:       \n\t" + result_data.map( tup => tup._1.toString + " ---> " + tup._2.mkBoundedDataString(", ",16) ).mkString("\n\t") )
  }

  //  test("pyMaxTestLocal") {
//    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"file:///Users/tpmaxwel/.cdas/cache/collections/NCML/MERRA_DAILY.ncml","name":"t:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"tzyx"}]]"""
//    val result_node = executeTest(datainputs)
//    val result_data = CDFloatArray( getResultData( result_node ) )
//    println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
//    //    println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
//    //    assert( result_data.maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
//  }

  test("pyTimeAveTest") {
    val nco_result: CDFloatArray = CDFloatArray( Array( 286.2326, 286.5537, 287.2408, 288.1576, 288.9455, 289.5202, 289.6924, 289.5549, 288.8497, 287.8196, 286.8923 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.ave","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkBoundedDataString(", ",10) )
//    println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
//    assert( result_data.maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
  }

  test("pyWeightedAveTestExt") {
      val nco_result: CDFloatArray = CDFloatArray( Array( 286.2326, 286.5537, 287.2408, 288.1576, 288.9455, 289.5202, 289.6924, 289.5549, 288.8497, 287.8196, 286.8923 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.cdmsExt.ave","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = CDFloatArray( getResultData( result_node, true ) )
      println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
      println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
    }


    test("pyRegridTest") {
      val unverified_result: CDFloatArray = CDFloatArray( Array( 238.94734, 238.95024, 238.95496, 238.95744, 238.95612, 238.95665, 238.95854, 238.95789, 238.95601, 238.95627, 238.95576, 238.95413, 238.95435, 238.95703, 238.95584, 238.95236, 238.94908, 238.94554, 238.94348, 238.94159, 238.94058, 238.93684, 238.93082, 238.92488, 238.91869, 238.9234, 238.92516, 238.91739, 238.91312, 238.91335, 238.91077, 238.90666, 238.902, 238.89793, 238.90051 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.cdmsModule.regrid","input":"v1","domain":"d0","crs":"gaussian~128"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = CDFloatArray( getResultData( result_node ) ).sample( 35 )
      println( " ** CDMS Result:       " + result_data.mkDataString( ", " ) )
      println( " ** Unverified Result:       " + unverified_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Regrid result does not match previously computed  value")
    }

    test("subsetTestT") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 295.6538,295.7205,295.9552,295.3324,293.0879,291.5541,289.6255,288.7875,289.7614,290.5001,292.3553,293.8378,296.7862,296.6005,295.6378,294.9304,293.6324,292.1851,290.8981,290.5262,290.5347,291.6595,292.8715,294.0839,295.4386,296.1736,296.4382,294.7264,293.0489,291.6237,290.5149,290.1141,289.8373,290.8802,292.615,294.0024,295.5854,296.5497,296.4013,295.1263,293.2203,292.2885,291.0839,290.281,290.1516,290.7351,292.7598,294.1442,295.8959,295.8112,296.1058,294.8028,292.7733,291.7613,290.7009,290.7226,290.1038,290.6277,292.1299,294.4099,296.1226,296.5852,296.4395,294.7828,293.7856,291.9353,290.2696,289.8393,290.3558,290.162,292.2701,294.3617,294.6855,295.9736,295.9881,294.853,293.4628,292.2583,291.2488,290.84,289.9593,290.8045,291.5576,293.0114,294.7605,296.3679,295.6986,293.4995,292.2574,290.9722,289.9694,290.1006,290.2442,290.7669,292.0513,294.2266,295.9346,295.6064,295.4227,294.3889,292.8391 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","lat":{"start":30,"end":30,"system":"indices"},"lon":{"start":30,"end":30,"system":"indices"},"time":{"start":0,"end":100,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
      val result_node = executeTest(datainputs)
      assert( getResultData( result_node ).maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
    }

  test("ESGF_subDemo1") {
    val unverified_result: CDFloatArray = CDFloatArray(  Array( 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841 ).map(_.toFloat), Float.MaxValue )
    val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
    val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
    val datainputs = s"""[
             variable=[$GISS_H_variables],
             domain=[       {"name":"d0","time":{"start":"1985-01-01T00:00:00Z","end":"1985-04-04T00:00:00Z","system":"values"}},{"name":"d1","crs":"gaussian~128"}],
             operation=[    {"name":"CDSpark.multiAverage","input":"${GISS_H_vids.mkString(",")}","domain":"d0","id":"eaGISS-H"} ]
            ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node, false ).slice(0,0,10) )
    println( " ** Op Result:         " + result_data.mkDataString(", ") )
    println( " ** Unverified Result: " + unverified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Max")
  }

  test("ESGF_subDemo2") {
    val unverified_result: CDFloatArray = CDFloatArray(  Array( 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841 ).map(_.toFloat), Float.MaxValue )
    val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
    val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
    val datainputs = s"""[
             variable=[$GISS_H_variables],
             domain=[       {"name":"d0","time":{"start":"1985-01-01T00:00:00Z","end":"1985-04-04T00:00:00Z","system":"values"}},{"name":"d1","crs":"gaussian~128"}],
             operation=[    {"name":"CDSpark.multiAverage","input":"${GISS_H_vids.mkString(",")}","domain":"d1","id":"eaGISS-H"} ]
            ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node, false ).slice(0,0,10) )
    println( " ** Op Result:         " + result_data.mkDataString(", ") )
    println( " ** Unverified Result: " + unverified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Max")
  }

//  test("ESGF_Demo") {
//      val unverified_result: CDFloatArray = CDFloatArray(  Array( 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908 ).map(_.toFloat), Float.MaxValue )
//      val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
//      val GISS_E2R_vids = ( 1 to nExp ) map { index => s"vR$index" }
//      val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
//      val GISS_E2R_variables = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss-e2-r_r${index}i1p1","name":"tas:${GISS_E2R_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
//      val datainputs = s"""[
//             variable=[$GISS_H_variables,$GISS_E2R_variables],
//             domain=[       {"name":"d0","time":{"start":"1985-01-01T00:00:00Z","end":"1985-04-04T00:00:00Z","system":"values"}},{"name":"d1","crs":"gaussian~128"}],
//             operation=[    {"name":"CDSpark.multiAverage","input":"${GISS_H_vids.mkString(",")}","domain":"d0","id":"eaGISS-H"},
//                            {"name":"CDSpark.multiAverage","input":"${GISS_E2R_vids.mkString(",")}","domain":"d0","id":"eaGISS-E2R"},
//                            {"name":"CDSpark.multiAverage","input":"eaGISS-E2R,eaGISS-H","domain":"d1","result":"esgfDemo"} ]
//            ]""".replaceAll("\\s", "")
//      val result_node = executeTest(datainputs)
//      val result_data = CDFloatArray( getResultData( result_node, false ).slice(0,0,10) )
//      println( " ** Op Result:         " + result_data.mkDataString(", ") )
//      println( " ** Unverified Result: " + unverified_result.mkDataString(", ") )
//      assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Max")
//    }

    test("TimeSum-dap") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 140615.5f, 139952f, 139100.6f, 138552.2f, 137481.9f, 137100.5f ), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0","cache":"false"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data )
      println( "Verified Result: " + nco_verified_result )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
    }

  test("TimeSum-dap-file") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 140615.5f, 139952f, 139100.6f, 138552.2f, 137481.9f, 137100.5f ), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0","cache":"false"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest( datainputs, Map( "response" -> "file" ) )
    val result_variable: Variable = getResultVariables( result_node ).head
    val result_data: CDFloatArray = CDFloatArray.factory( result_variable.read.reshape(Array(nco_verified_result.getSize)), Float.MaxValue )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
    println( "Verified Result: " + nco_verified_result.mkBoundedDataString(", ",100) )
    assert( nco_verified_result.maxScaledDiff( result_data )  < eps, s" Incorrect value computed for Max")
  }

  test("pyMaximum-cache") {
      val nco_verified_result = 309.7112
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node = executeTest(datainputs)
      val result_value = getResultValue(result_node)
      println( "Op Result:       " + result_value )
      println( "Verified Result: " + nco_verified_result )
      assert(Math.abs( result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
    }

    test("Maximum-cache") {
      val nco_verified_result = 309.7112
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node = executeTest(datainputs)
      val result_value = getResultValue(result_node)
      println("Op Result:       " + result_value)
      println("Verified Result: " + nco_verified_result)
      assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
    }

//  test("Maximum-local") {
//    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"file:///Users/tpmaxwel/.cdas/cache/collections/NCML/MERRA_DAILY.ncml","name":"t:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
//    val result_node = executeTest(datainputs, Map("response"->"file"))
//    println("Op Result:       " + printer.format( result_node ) )// result_data.mkBoundedDataString("[ ",", "," ]",100))
//  }

    test("Maximum-cache-twice") {
      val nco_verified_result = 309.7112
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node = executeTest(datainputs)
      val result_value = getResultValue(result_node)
      println("Op Result:       " + result_value)
      println("Verified Result: " + nco_verified_result)
      assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")

      val datainputs1 = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node1 = executeTest(datainputs1)
      val result_value1 = getResultValue(result_node1)
      println( "Op Result:       " + result_value1 )
      println( "Verified Result: " + nco_verified_result )
      assert(Math.abs( result_value1 - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
    }

    test("Maximum-dap") {
      val nco_verified_result = 309.7112
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node = executeTest(datainputs)
      val result_value = getResultValue(result_node)
      println( "Op Result:       " + result_value )
      println( "Verified Result: " + nco_verified_result )
      assert(Math.abs( result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
    }

  test("Seasons-filter") {
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":30,"end":50,"system":"indices"},"time":{"start":0,"end":200,"system":"indices"}}],variable=[{"uri":"file:///Users/tpmaxwel/.cdas/cache/collections/NCML/giss_r1i1p1.xml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","axes":"xt","filter":"DJF"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node, true )
    println( "Op Result:       " + result_data.toDataString )
  }

  test("pyTimeSum-dap") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 140615.5f, 139952f, 139100.6f, 138552.2f, 137481.9f, 137100.5f ), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.sum","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data )
      println( "Verified Result: " + nco_verified_result )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
    }

    test("TimeAve-dap") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 229.7638, 228.6798, 227.2885, 226.3925, 224.6436, 224.0204 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data )
      println( "Verified Result: " + nco_verified_result )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
    }

  test("SpaceAve-dap") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 270.07922, 269.5924, 267.8264, 265.12802, 262.94022, 261.35474, 260.80325, 260.67126, 261.85434, 263.46478, 265.88184, 269.2312, 270.60336, 270.05328, 267.67136, 265.6528, 263.22043, 262.25778, 261.28976, 261.22495, 260.86606, 263.05197, 265.86447, 269.4156, 270.377, 269.69855, 267.54056, 264.81995, 263.0417, 261.2425, 260.65546, 260.83783, 261.6036, 263.3008, 265.84967 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":25,"system":"indices"},"lon":{"start":5,"end":25,"system":"indices"}}],variable=[{"uri":"file:///Users/tpmaxwel/.cdas/cache/collections/NCML/giss_e2_r_r3i1p1.xml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest( datainputs, Map("numParts"->"4") )
    val result_data = getResultData( result_node ).sample(35)
    println( "Op Result:       " + result_data.mkBoundedDataString(", ", 35) )
    println( "Verified Result: " + nco_verified_result.mkBoundedDataString(", ", 35)  )
    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Max")
  }

  test("StdDev-dap") {
    // # NCO Verification script:
    //  datafile="http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc"
    //  ncks -O -v tas  -d lat,5,5 -d lon,5,10 ${datafile} ~/test/out/sample_data.nc
    //  ncwa -O -v tas -a time ~/test/out/sample_data.nc ~/test/out/time_ave.nc
    //  ncbo -O -v tas ~/test/out/sample_data.nc ~/test/out/time_ave.nc ~/test/out/dev.nc
    //  ncra -O -y rmssdn  ~/test/out/dev.nc ~/test/out/stdev.nc
    //  ncdump ~/test/out/stdev.nc

    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 8.891345, 9.084756, 9.556104, 9.460443, 10.18193, 10.28795 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[
            domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"}}],
            variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],
            operation=[       {"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t","id":"v1m"},
                              {"name":"CDSpark.diff2","input":"v1,v1m","domain":"d0","id":"v1ss"},
                              {"name":"CDSpark.rms","input":"v1ss","domain":"d0","axes":"t"}]
          ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ")  )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
  }

  test("TimeAve-npana") { if(use_npana_data) {
      val datainputs = """[domain=[{"name":"d0","lat":{"start":10,"end":20,"system":"indices"},"lon":{"start":10,"end":20,"system":"indices"}},{"name":"d1","lev":{"start":5,"end":5,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d1"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result Data:       " + result_data.mkBoundedDataString(", ", 64) )
    }}

    test("Maximum-file") {
      val nco_verified_result = 309.7112
      val data_file = "/data/GISS-r1i1p1-sample.nc"
      val uri=getClass.getResource(data_file)
      if( uri == null ) { throw new Exception( "Error locating file resource: " + data_file ) }
      else {
        val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"$uri","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
        val result_node = executeTest(datainputs)
        val result_value = getResultValue(result_node)
        println("Op Result:       " + result_value)
        println("Verified Result: " + nco_verified_result)
        assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
      }
    }

    test("pyMaximum-dap") {
      val nco_verified_result = 309.7112
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node = executeTest(datainputs)
      val result_value = getResultValue(result_node)
      println( "Op Result:       " + result_value )
      println( "Verified Result: " + nco_verified_result )
      assert(Math.abs( result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
    }

    test("Maximum1") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data.mkDataString(", ") )
      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
    }

    test("Maximum2") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Completed first execution, Result:       " + result_data.mkDataString(", ") )

      val result_node1 = executeTest(datainputs)
      val result_data1 = getResultData( result_node1 )
      println( "Completed second execution, Op Result:       " + result_data1.mkDataString(", ") )
      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
      assert( result_data1.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")

    }

    test("pyMaxT") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data.mkDataString(", ") )
      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
    }

//    test("pyMaxTCustom") {
//      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 275.95224, 277.0977, 277.9525, 278.9344, 280.25458, 282.28925, 283.88788, 285.12033, 285.94675, 286.6788, 287.6439 ).map(_.toFloat), Float.MaxValue )
//      val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.maxCustRed","input":"v1","domain":"d0","axes":"t"}]]"""
//      val result_node = executeTest(datainputs)
//      val result_data = getResultData( result_node )
//      println( "Op Result:       " + result_data.mkDataString(", ") )
//      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
//      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
//    }

    test("pyMaxTSerial") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.maxSer","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data.mkDataString(", ") )
      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
    }

    test("Minimum") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 214.3339, 215.8409, 205.9775, 208.0006, 206.4181, 202.4724, 202.9022, 206.9719, 217.8426, 215.4173, 216.0199, 217.2311, 231.4988, 231.5838, 232.7329, 232.5641 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":8,"system":"indices"},"lon":{"start":5,"end":8,"system":"indices"},"time":{"start":50,"end":150,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d0","axes":"t"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data.mkDataString(", ") )
      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Min")
    }

    test("subsetTestXY1") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array(   271.8525, 271.9948, 271.9691, 271.9805, 272.0052, 272.2418, 272.7861, 272.9485, 273.25, 273.4908, 273.5451, 273.45, 272.7733, 273.0835, 273.3886, 273.6199, 273.7051, 273.7632, 272.2565, 272.7566, 273.1762, 273.5975, 273.8943, 274.075, 272.4098, 272.8103, 273.2189, 273.6471, 273.8576, 274.0239, 273.3904, 273.5003, 273.667, 273.8236, 273.9353, 274.1161  ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","lat":{"start":10,"end":15,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data.mkDataString(", ") )
      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( nco_verified_result ) < eps, s" Incorrect value computed for Sum")
    }

    test("Max") {
      val nco_verified_result = 284.8936
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
      val result_node = executeTest(datainputs)
      val result_value = getResultValue(result_node)
      println( "Op Result:       " + result_value )
      println( "Verified Result: " + nco_verified_result )
      assert(Math.abs( result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
    }

    test("Max3") {
      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 296.312, 294.3597, 293.7058, 292.8994, 291.9226, 291.0488 ).map(_.toFloat), Float.MaxValue )
      val datainputs = s"""[domain=[{"name":"d0","lat":{"start":30,"end":40,"system":"values"},"time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"x"}]]"""
      val result_node = executeTest(datainputs)
      val result_data = getResultData( result_node )
      println( "Op Result:       " + result_data.mkDataString(", ") )
      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Min")
    }

  def readVerificationData( fileResourcePath: String, varName: String ): Option[CDFloatArray] = {
    try {
      val url = getClass.getResource( fileResourcePath ).toString
      logger.info( "Opening NetCDF dataset at url: " + url )
      val ncDataset: NetcdfDataset = NetcdfDataset.openDataset(url)
      val ncVariable = ncDataset.findVariable(varName)
      Some( CDFloatArray.factory(ncVariable.read(), Float.NaN) )
    } catch {
      case err: Exception =>
        println( "Error Reading VerificationData: " + err.getMessage )
        None
    }
  }

  def computeCycle( tsdata: CDFloatArray, cycle_period: Int ): CDFloatArray = {
    val values: CDFloatArray = CDFloatArray( Array(cycle_period), Array.fill[Float](cycle_period)(0f), Float.NaN )
    val counts: CDFloatArray = CDFloatArray( Array(cycle_period), Array.fill[Float](cycle_period)(0f), Float.NaN )
    for (index <- (0 until tsdata.getSize); val0 = tsdata.getFlatValue(index); if tsdata.valid(val0) ) {
      values.augment( Array(index % cycle_period), val0 )
      counts.augment( Array(index % cycle_period),  1f )
    }
    values / counts
  }

  def computeSeriesAverage( tsdata: CDFloatArray, ave_period: Int, offset: Int = 0, mod: Int = Int.MaxValue ): CDFloatArray = {
    val npts = tsdata.getSize / ave_period + 1
    val values: CDFloatArray = CDFloatArray( Array(npts), Array.fill[Float](npts)(0f), Float.NaN )
    val counts: CDFloatArray = CDFloatArray( Array(npts), Array.fill[Float](npts)(0f), Float.NaN )
    for (index <- (0 until tsdata.getSize); val0 = tsdata.getFlatValue(index); if tsdata.valid(val0) ) {
      val op_offset = (ave_period-offset) % ave_period
      val bin_index = ( ( index + op_offset ) / ave_period ) % mod
      values.augment( Array(bin_index), val0 )
      counts.augment( Array(bin_index), 1f )
    }
    values / counts
  }

  def getDataNodes( result_node: xml.Elem, print_result: Boolean = false  ): xml.NodeSeq = {
    if(print_result) { println( s"Result Node:\n${result_node.toString}\n" ) }
    result_node.label match {
      case "response" => result_node \\ "outputs" \\ "data"
      case _ => result_node \\ "Output" \\ "LiteralData"
    }
  }

  def getResultData( result_node: xml.Elem, print_result: Boolean = false ): CDFloatArray = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, print_result )
    try{  CDFloatArray( data_nodes.head.text.split(',').map(_.toFloat), Float.MaxValue ) } catch { case err: Exception => CDFloatArray.empty }
  }

  def getResultDataArraySeq( result_node: xml.Elem, print_result: Boolean = false ): Seq[(Int,CDFloatArray)] = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, print_result )
    data_nodes.map ( node => getNodeIndex(node) -> CDFloatArray( node.text.split(',').map(_.toFloat), Float.MaxValue ) ).sortBy( _._1 )
  }

  def getNodeIndex( node: xml.Node ): Int = node.attribute("id") match {
    case Some( idnode ) => idnode.text.split('.').last.toInt
    case None => -1
  }

  def getResultValue( result_node: xml.Elem ): Float = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node )
    try{ data_nodes.head.text.toFloat } catch { case err: Exception => Float.NaN }
  }

  def getResultVariables( result_node: xml.Elem ): List[Variable] = {
    val variables = ListBuffer.empty[Variable]
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, false )
    for (data_node <- data_nodes; if data_node.label.startsWith("data")) yield data_node.attribute("file") match {
      case None => Unit;
      case Some(filePath) => {
        val ncDataset: NetcdfDataset = NetcdfDataset.openDataset(filePath.toString)
        variables += ncDataset.findVariable("Nd4jMaskedTensor")
      }
    }
    variables.toList
  }

  def executeTest( datainputs: String, runArgs: Map[String,String]=Map.empty, identifier: String = "CDSpark.workflow"  ): xml.Elem = {
    val t0 = System.nanoTime()
    val runargs = runArgs ++ Map("responseform" -> "", "storeexecuteresponse" -> "true", "unitTest" -> "true" )
    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
    val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
    for( child_node <- response.child ) if ( child_node.label.startsWith("exception")) { throw new Exception( child_node.toString ) }
    println("Completed test '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    response
  }

  def cleanup() = {
    webProcessManager.shutdown( service )
  }

  def getCapabilities( identifier: String="", async: Boolean = false ): xml.Elem = {
    val t0 = System.nanoTime()
    val response: xml.Elem = webProcessManager.getCapabilities(service, identifier )
    webProcessManager.logger.info("Completed GetCapabilities '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    webProcessManager.logger.info( printer.format(response) )
    response
  }

  def describeProcess( identifier: String, async: Boolean = false ): xml.Elem = {
    val t0 = System.nanoTime()
    val response: xml.Elem = webProcessManager.describeProcess(service, identifier )
    webProcessManager.logger.info("Completed DescribeProcess '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    webProcessManager.logger.info( printer.format(response) )
    response
  }
}

class CDASDemoTestSuite extends FunSuite with Loggable with BeforeAndAfter {
  CDASLogManager.testing
  import nasa.nccs.cdapi.tensors.CDFloatArray
  import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
  import ucar.nc2.dataset.NetcdfDataset
  val serverConfiguration = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )
  
  def executeTest( datainputs: String, async: Boolean = false, identifier: String = "CDSpark.workflow" ): xml.Elem = {
    val t0 = System.nanoTime()
    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString, "unitTest" -> "true" )
    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
    val response: xml.Elem = webProcessManager.executeProcess("cds2", identifier, parsed_data_inputs, runargs)
    for( child_node <- response.child ) if ( child_node.label.startsWith("exception")) { throw new Exception( child_node.toString ) }
    println("Completed test '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    response
  }

  def getResultData( result_node: xml.Elem, print_result: Boolean = false ): CDFloatArray = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, print_result )
    try{  CDFloatArray( data_nodes.head.text.split(',').map(_.toFloat), Float.MaxValue ) } catch { case err: Exception => CDFloatArray.empty }
  }

  def getDataNodes( result_node: xml.Elem, print_result: Boolean = false  ): xml.NodeSeq = {
    if(print_result) { println( s"Result Node:\n${result_node.toString}\n" ) }
    result_node.label match {
      case "response" => result_node \\ "outputs" \\ "data"
      case _ => result_node \\ "Output" \\ "LiteralData"
    }
  }
}
/*

@Ignore class CDASMainTestSuite extends TestSuite(0, 0, 0f, 0f ) with Loggable {
//  Collections.addCollection( "merra.test", merra_data, "MERRA data", List("ta") )
//  Collections.addCollection( "const.test", const_data, "Constant data", List("ta") )

  test("GetCapabilities") {
    val result_node = getCapabilities("collections")
  }

  test("DescribeProcess") {
    val result_node = describeProcess( "CDSpark.min" )
  }

  test("Aggregate") {
    val collection = "GISS_r1i1p1"
    val url=getClass.getResource(s"/collections/GISS/$collection.csv")
    val GISS_path = url.getFile
    val datainputs = s"""[variable=[{"uri":"collection:/$collection","path":"$GISS_path"}]]"""
    val agg_result_node = executeTest(datainputs,false,"util.agg")
    logger.info( "Agg Result: " + printer.format(agg_result_node) )
  }

  test("AggregateFiles") {
    val collection = "MERRA_DAILY"
    val path = "/Users/tpmaxwel/Data/MERRA/DAILY"
    val datainputs = s"""[variable=[{"uri":"collection:/$collection","path":"$path"}]]"""
    val agg_result_node = executeTest(datainputs,false,"util.agg")
    logger.info( "Agg Result: " + printer.format(agg_result_node) )
  }

  test("Cache") {
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/GISS_r2i1p1","name":"tas:v1","domain":"d0"}]]"""
    val cache_result_node = executeTest(datainputs,false,"util.cache")
    logger.info( "Cache Result: " + printer.format(cache_result_node) )
  }

  test("CacheLocal") {
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}]]"""
    val cache_result_node = executeTest(datainputs,false,"util.cache")
    logger.info( "Cache Result: " + printer.format(cache_result_node) )
  }

  test("Aggregate&Cache") {
    val index = 6
    val collection = s"GISS_r${index}i1p1"
    val GISS_path = s"/Users/tpmaxwel/Dropbox/Tom/Data/ESGF-CWT/GISS/$collection.csv"
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/$collection","path":"${GISS_path}","name":"tas:v1","domain":"d0"}]]"""
    val agg_result_node = executeTest(datainputs,false,"util.agg")
    logger.info( "Agg Result: " + printer.format(agg_result_node) )
    val cache_result_node = executeTest(datainputs,false,"util.cache")
    logger.info( "Cache Result: " + printer.format(cache_result_node) )
  }

  test("EnsembleAve") {
    val variables = ( 1 to 6 ) map { index =>
      val collection = s"GISS_r${index}i1p1"
      val GISS_path = s"/Users/tpmaxwel/Dropbox/Tom/Data/ESGF-CWT/GISS/$collection.csv"
      s"""{"uri":"collection:/$collection","path":"${GISS_path}","name":"tas:v$index","domain":"d0"}"""
    }
    val vids = ( 1 to 6 ) map { index => s"v$index" }
    val datainputs = """[domain=[{"name":"d0"}],variable=[%s],operation=[{"name":"CDSpark.multiAverage","input":"%s","domain":"d0"}]]""".format( variables.mkString(","), vids.mkString(",") )
    logger.info( "Request datainputs: " + datainputs )
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    logger.info( "Sum1 Result: " + result_value.toString )
  }

  test("Sum") {
    val nco_verified_result = 4.886666e+07
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":0,"end":0,"system":"indices"},"time":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"collection:/MERRA_DAILY","name":"t:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Sum1") {
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    logger.info( "Sum1 Result: " + result_value.toString )
  }

  test("Sum Constant") {
    val nco_verified_result = 180749.0
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/const.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Maximum twice") {
    val nco_verified_result = 291.1066
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","result":"test_result","axes":"xy"}]]"""
    val result_node0 = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node0) )
    val result_node1 = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node1) )
    val data_nodes: xml.NodeSeq = result_node1 \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }
  test("SerializeTest") {
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.serializeTest","input":"v1","domain":"d0"}]]"""
    executeTest(datainputs) \\ "data"
  }
  test("Minimum") {
    val nco_verified_result = 239.4816
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p2","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
//    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text
//    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("MinimumFragment") {
    val lat_index = 50
    val lon_index = 100
    val datainputs = s"""[domain=[{"name":"d1","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}}],variable=[{"uri":"fragment:/t|merra___daily|0,0,0,0|248,1,144,288","name":"t:v1","domain":"d1"}],operation=[{"name":"CDSpark.min","input":"v1","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
  }

  test("OutOfBounds") {
    val lat_index = 50
    val lon_index = 100
    val lev_value = 75000
    val nco_verified_result = 239.4816
    val datainputs = s"""[domain=[{"name":"d1","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}},{"name":"d0","lev":{"start":$lev_value,"end":$lev_value,"system":"values"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d1","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
  }

  def getTimeseriesData( collId: String, varName: String, lon_index: Int, lat_index: Int, lev_index: Int): CDFloatArray = {
    val collection = new Collection( "aggregation", collId.replace('/','_'), "" )
    val cdvar = collection.getVariable(varName)
    val nTimesteps = cdvar.shape(0)
    val section: ma2.Section = new ma2.Section( Array(0,lev_index,lat_index,lon_index), Array(nTimesteps,1,1,1) )
    CDFloatArray( Array(nTimesteps), CDFloatArray.toFloatArray( collection.readVariableData( varName, section )), cdvar.missing )
  }

  test("Subset_Indexed_TS") {
    val lat_index = 50
    val lon_index = 100
    val lev_index = 0
    val direct_result_array = getTimeseriesData( "merra.test", "ta", lon_index, lat_index, lev_index )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"},"lev":{"start":$lev_index,"end":$lev_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_values = data_nodes.head.text.split(",").map( _.toFloat )
    val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
    val max_scaled_diff = result_array.maxScaledDiff( direct_result_array )
    printf( " \n\n        result, shape: " + result_array.getShape.mkString(",") + ", values: " + result_array.mkDataString(",") )
    printf( " \n\n direct result, shape: " + direct_result_array.getShape.mkString(",") + ", values: " + direct_result_array.mkDataString(",") )
    printf( "\n\n *** max_scaled_diff = " + max_scaled_diff )
    assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
  }

  test("Yearly Cycle") {
    val lat_index = 50
    val lon_index = 100
    val lev_index = 0
    val direct_result_array = getTimeseriesData( "merra.test", "ta", lon_index, lat_index, lev_index )
    val datainputs = s"""[domain=[{"name":"d2","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}},{"name":"d0","lev":{"start":$lev_index,"end":$lev_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.timeBin","input":"v1","result":"cycle","domain":"d2","axes":"t","bins":"t|month|ave|year"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_values: Array[Float] = data_nodes.head.text.trim.split(' ').head.split(',').map( _.toFloat )
    val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
    val computed_result = computeCycle( direct_result_array, 12 )
    val max_scaled_diff = result_array.maxScaledDiff( computed_result)
    printf( "    cdas result: " + result_array.mkDataString(",") + "\n" )
    printf( "computed result: " + computed_result.mkDataString(",") + "\n *** max_scaled_diff = " + max_scaled_diff )
    assert(max_scaled_diff < eps, s" Incorrect series computed for Yearly Cycle")
  }

  test("Workflow: Yearly Cycle Anomaly") {
    val lat_index = 50
    val lon_index = 100
    val lev_index = 0
    val direct_result_array = getTimeseriesData( "merra.test", "ta", lon_index, lat_index, lev_index )
    val datainputs = s"""[domain=[{"name":"d2","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}},{"name":"d0","lev":{"start":$lev_index,"end":$lev_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.timeBin","input":"v1","result":"cycle","domain":"d2","axes":"t","bins":"t|month|ave|year"},{"name":"CDSpark.diff2","input":["v1","cycle"],"domain":"d2","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_values: Array[Float] = data_nodes.head.text.trim.split(' ').head.split(',').map( _.toFloat )
    val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
    val computed_result = computeCycle( direct_result_array, 12 )
    val max_scaled_diff = result_array.maxScaledDiff( computed_result )
    printf( "    cdas result: " + result_array.mkDataString(",") + "\n" )
    printf( "computed result: " + computed_result.mkDataString(",") + "\n *** max_scaled_diff = " + max_scaled_diff )
    assert(max_scaled_diff < eps, s" Incorrect series computed for Yearly Cycle")
  }

  //  test("Subset(d0)") {
//    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
//      case Some(nco_verified_result) =>
//        val datainputs = s"""[domain=[{"name":"d0","lat":{"start":$lat_value,"end":$lat_value,"system":"values"},"lon":{"start":$lon_value,"end":$lon_value,"system":"values"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","axes":"t"}]]"""
//        val result_node = executeTest(datainputs) \\ "data"
//        val result_values = result_node.text.split(",").map( _.toFloat )
//        val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
//        printf( "nco_verified_result: " + nco_verified_result.mkDataString(",") )
//        val max_scaled_diff = maxScaledDiff(result_array, nco_verified_result)
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
//      case None => throw new Exception( "Can't read verification data")
//    }
//  }

  test("Spatial Average Constant") {
    val nco_verified_result = 1.0
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/const.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","weights":"","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "Data" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Weighted Spatial Average Constant") {
    val nco_verified_result = 1.0
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/const.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","weights":"cosine","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "Data" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Spatial Average") {
    val nco_verified_result = 270.092
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","weights":"","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq =  result_node \\ "Output" \\ "Data" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Weighted Spatial Average") {
    val nco_verified_result = 275.4043
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","weights":"cosine","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq =  result_node \\ "Output" \\ "Data" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  //  test("Seasonal Cycle") {
  //    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
  //      case Some( nco_subsetted_timeseries ) =>
  //        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "unit"->"month"), ( "period"->"3"), ( "mod"->"4"), ( "offset"->"2") )
  //        val result_values = computeArray("CDSpark.timeBin", dataInputs)
  //        val nco_verified_result = computeSeriesAverage( nco_subsetted_timeseries, 3, 2, 4 )
  //        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
  //        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
  //        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Yearly Cycle")
  //      case None => fail( "Error reading verification data")
  //    }
  //  }


  //  test("Subset(d0)") {
  //    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
  //      case Some( nco_verified_result ) =>
  //        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "axes"->"t") )
  //        val result_values = computeArray("CDSpark.subset", dataInputs)
  //        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
  //        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
  //        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
  //      case None => fail( "Error reading verification data")
  //    }
  //  }


//  test("Persistence") {
//    val dataInputs = getSubsetDataInputs( merra_data )
//    val request_context: RequestContext = getRequestContext( "CDSpark.metadata", dataInputs )
//    for( ospec <- request_context.inputs.values.flatten ) {
//      FragmentPersistence.deleteEnclosing(ospec)
//    }
//    val result_array1: CDFloatArray = computeArray("CDSpark.subset", dataInputs)
//    collectionDataCache.clearFragmentCache
//    val result_array2: CDFloatArray = computeArray("CDSpark.subset", dataInputs)
//    val max_diff = maxDiff( result_array1, result_array2 )
//    println(s"Test Result: %.4f".format( max_diff ) )
//    assert(max_diff == 0.0, " Persisted data differs from original data" )
//  }
//
//  test("Anomaly") {
//    readVerificationData( "/data/ta_anomaly_0_0.nc", "ta" ) match {
//      case Some( nco_verified_result ) =>
//        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "axes"->"t") )
//        val result_values = computeArray("CDSpark.anomaly", dataInputs)
//        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
//        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Anomaly")
//      case None => fail( "Error reading verification data")
//    }
//  }
//

//
//  test("Subset(d0) with secondary domain (d1)") {
//    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
//      case Some( nco_verified_result ) =>
//        val time_index = 3
//        val verified_result_array = nco_verified_result.section( Array(time_index,0,0,0), Array(1,1,1,1) )
//        val dataInputs = getTemporalDataInputs(merra_data, time_index, ( "domain"->"d1") )
//        val result_values = computeArray("CDSpark.subset", dataInputs)
//        val max_scaled_diff = maxScaledDiff(result_values,verified_result_array)
//        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), verified_result_array.toString(), max_scaled_diff))
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
//      case None => fail( "Error reading verification data")
//    }
//  }
//

//
////  test("Variable Metadata") {
////    val dataInputs = getMetaDataInputs( "collection://MERRA/mon/atmos", "ta" )
////    val result_node = computeXmlNode("CDSpark.metadata", dataInputs)
////    result_node.attribute("shape") match {
////      case Some( shape_attr ) => assert( shape_attr.text == "[432 42 361 540]", " Incorrect shape attribute, should be [432 42 361 540]: " + shape_attr.text )
////      case None => fail( " Missing 'shape' attribute in result: " + result_node.toString )
////    }
////  }
//

//
//  test("Weighted Masked Spatial Average") {
//    val nco_verified_result = 275.4317
//    val dataInputs = getMaskedSpatialDataInputs(merra_data, ( "axes"->"xy"), ( "weights"->"cosine") )
//    val result_value: Float = computeValue("CDSpark.average", dataInputs)
//    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
//    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Weighted Masked Spatial Average")
//  }
//
//
//  test("Yearly Means") {
//    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
//      case Some( nco_subsetted_timeseries ) =>
//        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "unit"->"month"), ( "period"->"12") )
//        val result_values = computeArray("CDSpark.timeBin", dataInputs)
//        val nco_verified_result = computeSeriesAverage( nco_subsetted_timeseries, 12 )
//        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
//        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Yearly Ave")
//        assert( result_values.getSize == 11, "Wrong size result in Yearly Means")
//      case None => fail( "Error reading verification data")
//    }
//  }
//

}

//object MinimumTest extends App {
//  val nco_verified_result = 239.4816
//  val datainputs = s"""[domain=[{"name":"d0","lev":{"start":0,"end":0,"system":"indices"},"time":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p2","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d0","axes":"xy"}]]"""
//  val result_node = executeTest(datainputs)
//  //    logger.info( "Test Result: " + printer.format(result_node) )
//  val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
//  val result_value = data_nodes.head.text
//  //    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
//
//  def executeTest( datainputs: String, async: Boolean = false, identifier: String = "CDSpark.workflow" ): xml.Elem = {
//    val t0 = System.nanoTime()
//    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString )
//    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
//    val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
//    webProcessManager.logger.info("Completed request '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
//    response
//  }
//}*/
