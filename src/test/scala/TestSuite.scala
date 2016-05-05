import nasa.nccs.cdapi.kernels.{BlockingExecutionResult, ErrorExecutionResult}
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.engine.CDS2ExecutionManager
import nasa.nccs.esgf.process.TaskRequest
import org.scalatest._
import ucar.nc2.dataset.NetcdfDataset

class TestSuite( val level_index: Int, val time_index: Int,   val lat_value: Float, val lon_value : Float ) extends FunSuite with Matchers {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val eps = 0.0002
  val merra_data = getClass.getResource("/data/MERRA_TEST_DATA_ta.nc").toString
  val const_data = getClass.getResource("/data/ConstantTestData.ta.nc").toString
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

  def maxScaledDiff(array0: CDFloatArray, array1: CDFloatArray): Float = {
    var max_diff = 0f
    var magsum = 0.0
    var count = 0
    val length = Math.min(array0.getSize, array1.getSize)
    for (index <- (0 until length); val0 = array0.getFlatValue(index); if val0 != array0.getInvalid; val1 = array1.getFlatValue(index); if val1 != array1.getInvalid; diff = Math.abs(val0 - val1)) {
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

  def getTemporalDataInputs(test_dataset: String, op_args: String) = Map(
    "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> lat_value, "end" -> lat_value, "system" -> "value"), "lon" -> Map("start" -> lon_value, "end" -> lon_value, "system" -> "values"))),
    "variable" -> List(Map("uri" -> test_dataset, "name" -> "ta:v0", "domain" -> "d0")),
    "operation" -> List(Map("unparsed" -> s"( v0, $op_args )")))

  def execute( kernel_name: String, data_inputs: Map[String, Seq[Map[String, Any]]] ): CDFloatArray = {
    val request = TaskRequest( kernel_name, data_inputs )
    cds2ExecutionManager.blockingExecute (request, run_args).results (0) match {
      case result: BlockingExecutionResult => result.result_tensor
      case err: ErrorExecutionResult => fail ( err.fatal() )
    }
  }
}
