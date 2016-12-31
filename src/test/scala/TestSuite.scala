import java.net.URI

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdas.workers.python.PythonWorkerPortal
import nasa.nccs.cds2.utilities.appParameters
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.utilities.Loggable
import org.scalatest._
import ucar.ma2
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

  def getResultData( result_node: xml.Elem ): CDFloatArray = {
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    CDFloatArray( data_nodes.head.text.split(',').map(_.toFloat), Float.MaxValue )
  }

  def getResultValue( result_node: xml.Elem ): Float = {
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    data_nodes.head.text.toFloat
  }

  def executeTest( datainputs: String, async: Boolean = false, identifier: String = "CDSpark.workflow" ): xml.Elem = {
    val t0 = System.nanoTime()
    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString, "unitTest" -> "true" )
    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
    val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
    webProcessManager.logger.info("Completed request '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
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


