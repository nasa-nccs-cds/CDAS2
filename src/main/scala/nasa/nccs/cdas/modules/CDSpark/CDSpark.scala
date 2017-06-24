package nasa.nccs.cdas.modules.CDSpark

import nasa.nccs.cdapi.data.{HeapFltArray, RDDRecord}
import ucar.ma2
import nasa.nccs.cdapi.tensors.{CDFloatArray, CDIndexMap}
import nasa.nccs.cdas.kernels._
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import ucar.ma2.DataType

import scala.reflect.runtime.{universe => u}

//package object CDSpark {
//  val name = "CDSpark"
//  val version = "1.0-SNAPSHOT"
//  val organization = "nasa.nccs"
//  val author = "Thomas Maxwell"
//  val contact = "thomas.maxwell@nasa.gov"
//}

class max extends SingularRDDKernel(Map("mapreduceOp" -> "max")) {
//  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Maximum"
  val description = "Computes maximum element value from input variable data over specified axes and roi"
  override val initValue: Float = -Float.MaxValue
}

//  class const extends SingularRDDKernel {
//    val inputs = List(Port("input fragment", "1"))
//    val outputs = List(Port("result", "1"))
//    override val description = "Sets Input Fragment to constant value"
//
//    override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: OperationContext ): Option[DataFragment] = {
//      inputs.head.map( dataFrag => {
//        val axes: AxisIndices = context.getAxisIndices (context.config ("axes", "") )
//        val async = context.config ("async", "false").toBoolean
//        val resultFragSpec = dataFrag.getReducedSpec (axes)
//        val sval = context.config ("value", "1.0")
//        val t10 = System.nanoTime
//        val result_val_masked: CDFloatArray = (dataFrag.data := sval.toFloat)
//        val t11 = System.nanoTime
//        logger.info ("Constant op, time = %.4f s, result sample = %s".format ((t11 - t10) / 1.0E9, getDataSample(result_val_masked).mkString(",").toString) )
//        DataFragment (resultFragSpec, result_val_masked)
//      } )
//    }
//  }



class partition extends Kernel() {
  val inputs = List( WPSDataInput("input variables", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Partitioner"
  val description = "Configures various data partitioning and filtering operations"
}

class min2 extends DualRDDKernel(Map("mapOp" -> "min")) {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Minimum"
  val description = "Computes element-wise minimum values for a pair of input variables data over specified roi"
}

class max2 extends DualRDDKernel(Map("mapOp" -> "max")) {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val description = "Computes element-wise maximum values for a pair of input variables data over specified roi"
}

class sum2 extends DualRDDKernel(Map("mapOp" -> "sum")) {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val description = "Computes element-wise sums for a pair of input variables data over specified roi"
}

class diff2 extends DualRDDKernel(Map("mapOp" -> "subt")) {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Difference"
  val description = "Computes element-wise diffs for a pair of input variables over specified roi"
}

class mult2 extends DualRDDKernel(Map("mapOp" -> "mult")) {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Product"
  val description = "Computes element-wise products for a pair of input variables data over specified roi"
}

class div2 extends DualRDDKernel(Map("mapOp" -> "divide")) {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Division"
  val description = "Computes element-wise divisions for a pair of input variables data over specified roi"
}

class min extends SingularRDDKernel(Map("mapreduceOp" -> "min")) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Minimum"
  val description = "Computes minimum element value from input variable data over specified axes and roi"
  override val initValue: Float = Float.MaxValue

}

class sum extends SingularRDDKernel(Map("mapreduceOp" -> "sum")) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Sum"
  val description = "Computes sums of element values from input variable data over specified axes and roi"
  override val initValue: Float = 0f
}

class rmSum extends SingularRDDKernel(Map("mapreduceOp" -> "sum","postOp"->"rms")) {
  val inputs = List( WPSDataInput("input variables", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Root Mean Sum"
  val description = "Computes root mean sum of input variable over specified axes and roi"
}

class rms extends SingularRDDKernel( Map("mapOp" -> "sqAdd", "reduceOp" -> "sum", "postOp"->"rms" ) ) {
  val inputs = List( WPSDataInput("input variables", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Root Mean Square"
  val description = "Computes root mean square of input variable over specified axes and roi"
}

class multiAverage extends Kernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 2, Integer.MAX_VALUE ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Ensemble Mean"
  val description = "Computes point-by-point average over inputs withing specified ROI"

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val async = context.config("async", "false").toBoolean
    val input_arrays: List[HeapFltArray] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[HeapFltArray]())( _ ++ _ )
    val input_ucarrays: Array[ma2.Array] = input_arrays.map(_.toUcarFloatArray).toArray
    assert( input_ucarrays.size > 1, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format( context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",") ) )
    val missing = input_arrays.head.getMissing()
    val resultArray: ma2.Array = ma2.Array.factory( DataType.FLOAT, input_ucarrays.head.getShape )
    var sum: Float = 0f
    var weight: Int = 0
    var fval: Float = 0f
    for( ipoint: Int <- ( 0 until input_ucarrays.head.getSize.toInt ) ) {
      sum = 0f; weight = 0
      for( ivar: Int <- (0 until input_ucarrays.length) ) {
        fval = input_ucarrays(ivar).getFloat(ipoint)
        if (fval != missing) {
          sum = sum + fval
          weight = weight + 1
        }
      }
      if( weight > 0 ) { resultArray.setFloat(ipoint,sum/weight) }
      else { resultArray.setFloat(ipoint,missing) }
    }
    logger.info("&MAP: Finished Kernel %s, inputs = %s, output = %s, time = %.4f s".format(name, context.operation.inputs.mkString(","), context.operation.rid, (System.nanoTime - t0)/1.0E9) )
    context.addTimestamp( "Map Op complete" )
    val result_metadata = input_arrays.head.metadata ++ List( "uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile( inputs.elements )  )
    RDDRecord( Map( context.operation.rid -> HeapFltArray( resultArray, input_arrays(0).origin, input_arrays.head.gridSpec, result_metadata, missing) ), inputs.metadata )
  }
}

class average extends SingularRDDKernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Mean"
  val description = "Computes (weighted) means of element values from input variable data over specified axes and roi"

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axes = context.config("axes","")
    val axisIndices: Array[Int] = context.grid.getAxisIndices( axes ).getAxes.toArray
    val elems = context.operation.inputs.map( inputId => inputs.element(inputId) match {
      case Some( input_data ) =>
        val input_array = input_data.toCDFloatArray
        val (weighted_value_sum_masked, weights_sum_masked) =  if( addWeights(context) ) {
          val weights: CDFloatArray = KernelUtilities.getWeights(inputId, context)
          input_array.weightedSum(axisIndices,Some(weights))
        } else {
          input_array.weightedSum(axisIndices,None)
        }
        context.operation.rid -> HeapFltArray(weighted_value_sum_masked, input_data.origin, arrayMdata(inputs, "value"), Some(weights_sum_masked.getArrayData()))
      case None => throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
    })
    logger.info("Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9))
    context.addTimestamp( "Map Op complete" )
    val rv = RDDRecord( Map( elems:_*), inputs.metadata ++ List( "rid" -> context.operation.rid, "axes" -> axes.toUpperCase ) )
    logger.info("Returning result value")
    rv
  }

  def map1 ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axes = context.config("axes","")
    val axisIndices: AxisIndices = context.grid.getAxisIndices( axes )
    val async = context.config("async", "false").toBoolean
    val elems = context.operation.inputs.map( inputId => inputs.element(inputId) match {
      case Some( input_data ) =>
        val input_array = input_data.toCDFloatArray
        val accumulation_index = input_array.getAccumulationIndex( axisIndices.args )
        val weights: CDFloatArray = KernelUtilities.getWeights(inputId, context)
        val (weighted_value_sum_masked, weights_sum_masked) = input_array.weightedReduce( CDFloatArray.getOp("add"), 0f, accumulation_index, Some(weights) )
        context.operation.rid -> HeapFltArray( weighted_value_sum_masked, input_data.origin, arrayMdata(inputs, "value"), Some(weights_sum_masked.getArrayData()) )
      case None => throw new Exception( "Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(",") )
    })
    logger.info("Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9))
    context.addTimestamp( "Map Op complete" )
    val rv = RDDRecord( Map( elems:_*), inputs.metadata ++ List( "rid" -> context.operation.rid, "axes" -> axes.toUpperCase ) )
    logger.info("Returning result value")
    rv
  }
  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}

class subset extends Kernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Subset"
  val description = "Extracts a subset of element values from input variable data over the specified axes and roi"
}

class timeBin extends Kernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Time Binning"
  override val description = "Aggregates data into bins over time using specified reduce function and binning specifications"

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val period = context.config("period", "1" ).toInt
    val mod = context.config("mod", "12" ).toInt
    val unit = context.config("unit", "month" )
    val offset = context.config("offset", "0" ).toInt
    val async = context.config("async", "false").toBoolean
    val ( id, input_array ) = inputs.head
    val accumulation_index: CDIndexMap = input_array.toCDFloatArray.getIndex.getAccumulator( axes.args, List( getMontlyBinMap( id, context ) )  )  // TODO: Check range of getMontlyBinMap- subset by part?
    val (weighted_value_sum_masked, weights_sum_masked) = input_array.toCDFloatArray.weightedReduce( CDFloatArray.getOp("add"), 0f, accumulation_index )
    val result_array = HeapFltArray( weighted_value_sum_masked, input_array.origin, arrayMdata(inputs,"value"), Some( weights_sum_masked.getArrayData() ) )
    logger.info("Executed Kernel %s map op, input = %s, index=%s, time = %.4f s".format(name, id, result_array.toCDFloatArray.getIndex.toString , (System.nanoTime - t0) / 1.0E9))
    context.addTimestamp( "Map Op complete" )
    RDDRecord( Map( context.operation.rid -> result_array ), inputs.metadata ++ List( "rid" -> context.operation.rid ) )
  }
  override def combineRDD(context: KernelContext)( a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)( a0, a1 )
  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}


//  class anomaly extends SingularRDDKernel {
//    val inputs = List(Port("input fragment", "1"))
//    val outputs = List(Port("result", "1"))
//    override val description = "Anomaly over Input Fragment"
//
//    override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: OperationContext ): Option[DataFragment] = {
//      inputs.head.map( dataFrag => {
//        val async = context.config("async", "false").toBoolean
//        val axes: AxisIndices = context.getAxisIndices(context.config("axes", ""))
//        val resultFragSpec = dataFrag.getReducedSpec(axes)
//        val t10 = System.nanoTime
//        val weighting_type = context.config("weights", if (context.config("axis", "").contains('y')) "cosine" else "")
//        val weightsOpt: Option[CDFloatArray] = weighting_type match {
//          case "" => None
//          case wtype => context.targetGrid.getAxisData( 'y', dataFrag.spec.roi ).map(axis_data => dataFrag.data.computeWeights(wtype, Map('y' -> axis_data)))
//        }
//        val anomaly_result: CDFloatArray = dataFrag.data.anomaly(axes.args, weightsOpt)
//        logger.info( "Partition[%d], generated anomaly result: %s".format(partIn
// dex, anomaly_result.toDataString ) )
//        val t11 = System.nanoTime
//        DataFragment(resultFragSpec, anomaly_result)
//      } )
//    }
//  }

