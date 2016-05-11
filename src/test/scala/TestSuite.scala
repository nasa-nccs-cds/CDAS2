import nasa.nccs.cdapi.kernels.{BlockingExecutionResult, ErrorExecutionResult, ExecutionResult, XmlExecutionResult}
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.engine.CDS2ExecutionManager
import nasa.nccs.esgf.process.TaskRequest
import org.scalatest._
import ucar.nc2.dataset.NetcdfDataset

class TestSuite( val level_index: Int, val time_index: Int,   val lat_value: Float, val lon_value : Float ) extends FunSuite with Matchers {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val eps = 0.0002
  val merra_data = getClass.getResource("/data/merra_test_data.ta.nc").toString
  val const_data = getClass.getResource("/data/constant_test_data.ta.nc").toString
  val run_args = Map("async" -> "false")

  def readVerificationData( fileResourcePath: String, varName: String ): Option[CDFloatArray] = {
    try {
      val url = getClass.getResource( fileResourcePath ).toString
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
    val values: CDFloatArray = new CDFloatArray( Array(cycle_period), Array.fill[Float](cycle_period)(0f), Float.NaN )
    val counts: CDFloatArray = new CDFloatArray( Array(cycle_period), Array.fill[Float](cycle_period)(0f), Float.NaN )
    for (index <- (0 until tsdata.getSize); val0 = tsdata.getFlatValue(index); if tsdata.valid(val0) ) {
      val bin_index = index % cycle_period
      values.setFlatValue(bin_index, values.getFlatValue(bin_index) + val0 )
      counts.setFlatValue(bin_index, counts.getFlatValue(bin_index) + 1f )
    }
    values / counts
  }

  def computeSeriesAverage( tsdata: CDFloatArray, ave_period: Int ): CDFloatArray = {
    val npts = tsdata.getSize / ave_period + 1
    val values: CDFloatArray = new CDFloatArray( Array(npts), Array.fill[Float](npts)(0f), Float.NaN )
    val counts: CDFloatArray = new CDFloatArray( Array(npts), Array.fill[Float](npts)(0f), Float.NaN )
    for (index <- (0 until tsdata.getSize); val0 = tsdata.getFlatValue(index); if tsdata.valid(val0) ) {
      val bin_index = index / ave_period
      values.setFlatValue(bin_index, values.getFlatValue(bin_index) + val0 )
      counts.setFlatValue(bin_index, counts.getFlatValue(bin_index) + 1f )
    }
    values / counts
  }

  def maxScaledDiff(array0: CDFloatArray, array1: CDFloatArray): Float = {
    var max_diff = 0f
    var magsum = 0.0
    var count = 0
    val length = Math.min(array0.getSize, array1.getSize)
    val a0 = array0.getSectionData
    val a1 = array1.getSectionData
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

  def getSpatialDataInputs(test_dataset: String, op_args: String) = Map(
    "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> time_index, "end" -> time_index, "system" -> "indices"))),
    "variable" -> List(Map("uri" -> test_dataset, "name" -> "ta:v0", "domain" -> "d0")),
    "operation" -> List(Map("unparsed" -> s"( v0, $op_args )")))

  def getTemporalDataInputs(test_dataset: String, op_args: String, d1_time_index: Int = time_index ) = Map(
    "domain" -> List(Map("name" -> "d1", "time" -> Map("start" -> d1_time_index, "end" -> d1_time_index, "system" -> "indices")
    ), Map("name" -> "d0", "lat" -> Map("start" -> lat_value, "end" -> lat_value, "system" -> "value"), "lon" -> Map("start" -> lon_value, "end" -> lon_value, "system" -> "values"))),
    "variable" -> List(Map("uri" -> test_dataset, "name" -> "ta:v0", "domain" -> "d0")),
    "operation" -> List(Map("unparsed" -> s"( v0, $op_args )")))

  def getMetaDataInputs(test_dataset: String, varName: String) = Map( "variable" -> List(Map("uri" -> test_dataset, "name" -> varName ) ) )

  def computeResult( kernel_name: String, data_inputs: Map[String, Seq[Map[String, Any]]] ): ExecutionResult = {
    val request = TaskRequest( kernel_name, data_inputs )
    cds2ExecutionManager.blockingExecute (request, run_args).results.head match {
      case result: BlockingExecutionResult => result
      case xmlresult: XmlExecutionResult => xmlresult
      case err: ErrorExecutionResult => fail ( err.fatal() )
      case x => fail ( "Unexpected response, got " + x.toString )
    }
  }

  def computeArray( kernel_name: String, data_inputs: Map[String, Seq[Map[String, Any]]] ): CDFloatArray =
    computeResult( kernel_name, data_inputs ) match {
      case result: BlockingExecutionResult => result.result_tensor
      case x => fail ( "Expecting Float Array response, got " + x.toString )
    }

  def computeXmlNode( kernel_name: String, data_inputs: Map[String, Seq[Map[String, Any]]] ): xml.Node =
    computeResult( kernel_name, data_inputs ) match {
      case result: XmlExecutionResult => result.responseXml
      case x => fail ( "Expecting xml response, got " + x.toString )
    }

  def computeValue( kernel_name: String, data_inputs: Map[String, Seq[Map[String, Any]]] ): Float = computeArray( kernel_name, data_inputs ).getValue(0)

}
