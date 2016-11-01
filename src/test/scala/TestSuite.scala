import java.net.URI

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.utilities.appParameters
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.utilities.Loggable
import org.scalatest._
import ucar.nc2.dataset.NetcdfDataset

class TestSuite( val level_index: Int, val time_index: Int,   val lat_value: Float, val lon_value : Float ) extends FunSuite with Matchers with Loggable  {
  val serverConfiguration = Map[String,String]()
  val configMap = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )
  val eps = 0.0002
  val service = "cds2"
  val demo_data_location = appParameters("demo.data.location","")
  val merra_data = demo_data_location + "/merra_test_data.ta.nc" // getClass.getResource("/data/merra_test_data.ta.nc").toString.split(":").last
  val const_data = demo_data_location + "/constant_test_data.ta.nc" // getClass.getResource("/data/constant_test_data.ta.nc").toString.split(":").last
  val run_args = Map("async" -> "false")
  val printer = new scala.xml.PrettyPrinter(200, 3)

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

  def maxScaledDiff(array0: CDFloatArray, array1: CDFloatArray): Float = {
    var max_diff = 0f
    var magsum = 0.0
    var count = 0
    val length = Math.min(array0.getSize, array1.getSize)
    val a0 = array0.getArrayData()
    val a1 = array1.getArrayData()
    for (index <- (0 until length); val0 = a0(index); if val0 != array0.getInvalid; val1 = a1(index); if val1 != array1.getInvalid; diff = Math.abs(val0 - val1)) {
      if (diff > max_diff) {
        max_diff = diff
      }
      magsum = magsum + Math.abs(val0)
      count = count + 1
    }
    val mag = magsum / count
    max_diff / mag.toFloat
  }

  def maxDiff(array0: CDFloatArray, array1: CDFloatArray): Float = {
    var max_diff = 0f
    val length = Math.min(array0.getSize, array1.getSize)
    val a0 = array0.getArrayData()
    val a1 = array1.getArrayData()
    for (index <- (0 until length); val0 = a0(index); if val0 != array0.getInvalid; val1 = a1(index); if val1 != array1.getInvalid; diff = Math.abs(val0 - val1)) {
      if (diff > max_diff) { max_diff = diff }
    }
    max_diff
  }

  def executeTest( datainputs: String, async: Boolean = false, identifier: String = "CDSpark.workflow" ): xml.Elem = {
    val t0 = System.nanoTime()
    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString )
    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
    val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
    webProcessManager.logger.info("Completed request '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    response
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

  def getSpatialDataInputs(test_dataset: String, op_args: (String,String)* )  = Map(
    "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> time_index, "end" -> time_index, "system" -> "indices"))),
    "variable" -> List(Map("uri" -> test_dataset, "name" -> "ta:v0", "domain" -> "d0")),
    "operation" -> List(Map( ( ("input"->"v0") :: op_args.toList ) :_* )))

  def getSubsetDataInputs(test_dataset: String, op_args: (String,String)* )  = Map(
    "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "lat" -> Map("start" -> -30.0, "end" -> 30.0, "system" -> "value"), "lon" -> Map("start" -> 0.0, "end" -> 60.0, "system" -> "values"))),
    "variable" -> List(Map("uri" -> test_dataset, "name" -> "ta:v0", "domain" -> "d0")),
    "operation" -> List(Map( ( ("input"->"v0") :: op_args.toList ) :_* )))

  def getMaskedSpatialDataInputs(test_dataset: String, op_args: (String,String)* ) = Map(
    "domain" -> List(Map("name" -> "d0", "mask" -> "#ocean50m", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> time_index, "end" -> time_index, "system" -> "indices"))),
    "variable" -> List(Map("uri" -> test_dataset, "name" -> "ta:v0", "domain" -> "d0")),
    "operation" ->  List(Map( ( ("input"->"v0") :: op_args.toList ) :_* )))

  def getTemporalDataInputs(test_dataset: String, d1_time_index: Int, op_args: (String,String)*  ) = Map(
    "domain" -> List(Map("name" -> "d1", "time" -> Map("start" -> d1_time_index, "end" -> d1_time_index, "system" -> "indices")
    ), Map("name" -> "d0", "lat" -> Map("start" -> lat_value, "end" -> lat_value, "system" -> "value"), "lon" -> Map("start" -> lon_value, "end" -> lon_value, "system" -> "values"))),
    "variable" -> List(Map("uri" -> test_dataset, "name" -> "ta:v0", "domain" -> "d0")),
    "operation" ->  List(Map( ( ("input"->"v0") :: op_args.toList ) :_* )))

  def getMetaDataInputs(test_dataset: String, varName: String) = Map(
    "variable" -> List(Map("uri" -> test_dataset, "name" -> varName )),
    "operation" ->  List(Map( ("input"->varName), ("name"->"CDSpark.metadata" ) )) )


}

object netcdfTestApp extends App {
  import ucar.nc2.dataset.NetcdfDataset
  val varName = "tas"
  val uri = new URI("http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc")
  println( s"Opening dataset " + uri )
  val ncDataset: NetcdfDataset = NetcdfDataset.openDataset( uri.toString )
  val ncVariable = ncDataset.findVariable(varName)
  println( s"Read variable $varName, shape = " + ncVariable.getShape.mkString(",") )
}
