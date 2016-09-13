import nasa.nccs.caching.{ FragmentPersistence, collectionDataCache }
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.esgf.process.RequestContext

class ExecutionSpec extends TestSuite(0, 0, 0f, 0f ) {

  test("Sum") {
    val nco_verified_result = 4.886666e+07
    val dataInputs = getSpatialDataInputs(merra_data, ( "axes"->"xy"))
    val result_value: Float = computeValue("CDS.sum", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Sum Constant") {
    val nco_verified_result = 180749.0
    val dataInputs = getSpatialDataInputs(const_data, ( "axes"->"xy"))
    val result_value: Float = computeValue("CDS.sum", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Maximum") {
    val nco_verified_result = 291.1066
    val dataInputs = getSpatialDataInputs(merra_data, ( "axes"->"xy"))
    val result_value: Float = computeValue("CDS.max", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Maximum")
  }

  test("Minimum") {
    val nco_verified_result = 239.4816
    val dataInputs = getSpatialDataInputs(merra_data, ( "axes"->"xy"))
    val result_value: Float = computeValue("CDS.min", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Minimum")
  }

  test("Persistence") {
    val dataInputs = getSubsetDataInputs( merra_data )
    val request_context: RequestContext = getRequestContext( "CDS.metadata", dataInputs )
    for( ospec <- request_context.inputs.values.flatten ) {
      FragmentPersistence.deleteEnclosing(ospec)
    }
    val result_array1: CDFloatArray = computeArray("CDS.subset", dataInputs)
    collectionDataCache.clearFragmentCache
    val result_array2: CDFloatArray = computeArray("CDS.subset", dataInputs)
    val max_diff = maxDiff( result_array1, result_array2 )
    println(s"Test Result: %.4f".format( max_diff ) )
    assert(max_diff == 0.0, " Persisted data differs from original data" )
  }

  test("Anomaly") {
    readVerificationData( "/data/ta_anomaly_0_0.nc", "ta" ) match {
      case Some( nco_verified_result ) =>
        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "axes"->"t") )
        val result_values = computeArray("CDS.anomaly", dataInputs)
        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Anomaly")
      case None => fail( "Error reading verification data")
    }
  }

  test("Subset(d0)") {
    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
      case Some( nco_verified_result ) =>
        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "axes"->"t") )
        val result_values = computeArray("CDS.subset", dataInputs)
        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
      case None => fail( "Error reading verification data")
    }
  }

  test("Subset(d0) with secondary domain (d1)") {
    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
      case Some( nco_verified_result ) =>
        val time_index = 3
        val verified_result_array = nco_verified_result.section( Array(time_index,0,0,0), Array(1,1,1,1) )
        val dataInputs = getTemporalDataInputs(merra_data, time_index, ( "domain"->"d1") )
        val result_values = computeArray("CDS.subset", dataInputs)
        val max_scaled_diff = maxScaledDiff(result_values,verified_result_array)
        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), verified_result_array.toString(), max_scaled_diff))
        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
      case None => fail( "Error reading verification data")
    }
  }

  test("Spatial Average") {
    val nco_verified_result = 270.092
    val dataInputs = getSpatialDataInputs(merra_data, ( "axes"->"xy"), ( "weights"->"") )
    val result_value: Float = computeValue("CDS.average", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Spatial Average")
  }

//  test("Variable Metadata") {
//    val dataInputs = getMetaDataInputs( "collection://MERRA/mon/atmos", "ta" )
//    val result_node = computeXmlNode("CDS.metadata", dataInputs)
//    result_node.attribute("shape") match {
//      case Some( shape_attr ) => assert( shape_attr.text == "[432 42 361 540]", " Incorrect shape attribute, should be [432 42 361 540]: " + shape_attr.text )
//      case None => fail( " Missing 'shape' attribute in result: " + result_node.toString )
//    }
//  }

  test("Weighted Spatial Average") {
    val nco_verified_result = 275.4043
    val dataInputs = getSpatialDataInputs(merra_data, ( "axes"->"xy"), ( "weights"->"cosine") )
    val result_value: Float = computeValue("CDS.average", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Weighted Spatial Average")
  }

  test("Weighted Masked Spatial Average") {
    val nco_verified_result = 275.4317
    val dataInputs = getMaskedSpatialDataInputs(merra_data, ( "axes"->"xy"), ( "weights"->"cosine") )
    val result_value: Float = computeValue("CDS.average", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Weighted Masked Spatial Average")
  }

  test("Yearly Cycle") {
    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
      case Some( nco_subsetted_timeseries ) =>
        val dataInputs = getTemporalDataInputs( merra_data, 0, ( "unit"->"month"), ( "period"->"1"), ( "mod"->"12")  )
        val result_values = computeArray("CDS.timeBin", dataInputs)
        val nco_verified_result = computeCycle( nco_subsetted_timeseries, 12 )
        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Yearly Cycle")
      case None => fail( "Error reading verification data")
    }
  }

  test("Seasonal Cycle") {
    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
      case Some( nco_subsetted_timeseries ) =>
        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "unit"->"month"), ( "period"->"3"), ( "mod"->"4"), ( "offset"->"2") )
        val result_values = computeArray("CDS.timeBin", dataInputs)
        val nco_verified_result = computeSeriesAverage( nco_subsetted_timeseries, 3, 2, 4 )
        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Yearly Cycle")
      case None => fail( "Error reading verification data")
    }
  }

  test("Yearly Means") {
    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
      case Some( nco_subsetted_timeseries ) =>
        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "unit"->"month"), ( "period"->"12") )
        val result_values = computeArray("CDS.timeBin", dataInputs)
        val nco_verified_result = computeSeriesAverage( nco_subsetted_timeseries, 12 )
        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Yearly Ave")
        assert( result_values.getSize == 11, "Wrong size result in Yearly Means")
      case None => fail( "Error reading verification data")
    }
  }

  test("Spatial Average Constant") {
    val nco_verified_result = 1.0
    val dataInputs = getSpatialDataInputs(const_data, ( "axes"->"xy"), ( "weights"->"") )
    val result_value: Float = computeValue("CDS.average", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Spatial Average Constant")
  }

  test("Weighted Spatial Average Constant") {
    val nco_verified_result = 1.0
    val dataInputs = getSpatialDataInputs(const_data, ( "axes"->"xy"), ( "weights"->"cosine") )
    val result_value: Float = computeValue("CDS.average", dataInputs)
    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Spatial Average Constant with Weights")
  }
}