import nasa.nccs.caching.{FragmentPersistence, collectionDataCache}
import nasa.nccs.cdapi.cdm.Collection
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.wps.wpsObjectParser

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.utilities.{Loggable, cdsutils}
import ucar.ma2
import org.apache.log4j.{Level, LogManager, Logger}

class CurrentTestSuite extends TestSuite(0, 0, 0f, 0f ) with Loggable {

  test("Aggregate") {
    ( 1 to 5 ) map { index =>
      val collection = s"GISS_r${index}i1p1"
      val url = getClass.getResource(s"/collections/GISS/$collection.csv")
      val GISS_path = url.getFile
      val datainputs = s"""[variable=[{"uri":"collection:/$collection","path":"$GISS_path"}]]"""
      val agg_result_node = executeTest(datainputs, false, "util.agg")
      logger.info(s"Agg collection $collection Result: " + printer.format(agg_result_node))
    }
  }

  test("Cache") {
    ( 1 to 5 ) map { index =>
      val collection = s"GISS_r${index}i1p1"
      val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":100,"system":"indices"}}],variable=[{"uri":"collection:/$collection","name":"tas:v1","domain":"d0"}]]"""
      val cache_result_node = executeTest(datainputs, false, "util.cache")
      logger.info(s"Cache $collection:tas Result: " + printer.format(cache_result_node))
    }
  }

  test("subsetTestXY") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 241.2655, 241.2655, 241.2655, 241.2655, 241.2655, 241.2655, 245.2, 244.904, 244.6914, 244.5297, 244.2834, 244.0234, 245.4426, 245.1731, 244.9478, 244.6251, 244.2375, 244.0953, 248.4837, 247.4268, 246.4957, 245.586, 245.4244, 244.8213, 249.7772, 248.7458, 247.5331, 246.8871, 246.0183, 245.8848, 248.257, 247.3562, 246.3798, 245.3962, 244.6091, 243.6039 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":0,"end":5,"system":"indices"},"lon":{"start":0,"end":5,"system":"indices"},"time":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
    val result_node = executeTest(datainputs)
    assert( getResultData( result_node ).maxScaledDiff( nco_verified_result ) < eps, s" Incorrect value computed for Sum")
  }

  test("regridTest") {
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":1000,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.regrid","input":"v1","domain":"d0","crs":"gaussian~128"}]]"""
    val result_node = executeTest(datainputs)
  }

  test("subsetTestT") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 295.6538,295.7205,295.9552,295.3324,293.0879,291.5541,289.6255,288.7875,289.7614,290.5001,292.3553,293.8378,296.7862,296.6005,295.6378,294.9304,293.6324,292.1851,290.8981,290.5262,290.5347,291.6595,292.8715,294.0839,295.4386,296.1736,296.4382,294.7264,293.0489,291.6237,290.5149,290.1141,289.8373,290.8802,292.615,294.0024,295.5854,296.5497,296.4013,295.1263,293.2203,292.2885,291.0839,290.281,290.1516,290.7351,292.7598,294.1442,295.8959,295.8112,296.1058,294.8028,292.7733,291.7613,290.7009,290.7226,290.1038,290.6277,292.1299,294.4099,296.1226,296.5852,296.4395,294.7828,293.7856,291.9353,290.2696,289.8393,290.3558,290.162,292.2701,294.3617,294.6855,295.9736,295.9881,294.853,293.4628,292.2583,291.2488,290.84,289.9593,290.8045,291.5576,293.0114,294.7605,296.3679,295.6986,293.4995,292.2574,290.9722,289.9694,290.1006,290.2442,290.7669,292.0513,294.2266,295.9346,295.6064,295.4227,294.3889,292.8391 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":30,"end":30,"system":"indices"},"lon":{"start":30,"end":30,"system":"indices"},"time":{"start":0,"end":100,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
    val result_node = executeTest(datainputs)
    assert( getResultData( result_node ).maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
  }

  test("EnsembleAve") {
    val variables = ( 1 to 5 ) map { index => s"""{"uri":"collection:/GISS_r${index}i1p1","name":"tas:v$index","domain":"d0"}""" }
    val vids = ( 1 to 5 ) map { index => s"v$index" }
    val datainputs = """[domain=[{"name":"d0","time":{"start":0,"end":5,"system":"indices"}}],variable=[%s],operation=[{"name":"CDSpark.multiAverage","input":"%s","domain":"d0"}]]""".format( variables.mkString(","), vids.mkString(",") )
    val result_node = executeTest(datainputs)
  }

  test("Maximum") {
    val nco_verified_result = 309.7112
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    assert(Math.abs( getResultValue(result_node) - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
  }

  test("Minimum") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 211.1611, 209.252, 205.9775, 208.0006, 206.4181, 202.4724, 202.9022, 203.1199, 217.8426, 215.4173, 216.0199, 217.2311, 231.4988, 231.5838, 232.7329, 232.5641 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":8,"system":"indices"},"lon":{"start":5,"end":8,"system":"indices"},"time":{"start":0,"end":100,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    assert( getResultData( result_node ).maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Min")
  }

  test("Sum") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 42297.59, 42432.7, 42578.24, 42712.27, 42820.25, 42877.53, 42949.36, 43001.19, 43058.9, 43111.22, 43136.12 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":30,"end":40,"system":"indices"},"time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"x"}]]"""
    val result_node = executeTest(datainputs)
    assert( getResultData( result_node ).maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Min")
  }

}

class CDASMainTestSuite extends TestSuite(0, 0, 0f, 0f ) with Loggable {
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
//}