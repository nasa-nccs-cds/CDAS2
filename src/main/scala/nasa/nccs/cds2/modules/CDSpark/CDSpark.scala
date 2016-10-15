package nasa.nccs.cds2.modules.CDSpark

import nasa.nccs.cdapi.data.{HeapFltArray, RDDPartition}
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray._
import nasa.nccs.cdapi.tensors.{CDCoordMap, CDFloatArray, CDIndexMap, CDTimeCoordMap}
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.{universe => u}

//package object CDSpark {
//  val name = "CDSpark"
//  val version = "1.0-SNAPSHOT"
//  val organization = "nasa.nccs"
//  val author = "Thomas Maxwell"
//  val contact = "thomas.maxwell@nasa.gov"
//}

class max extends SingularRDDKernel {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Maximum"
  val description = "Computes maximum element value from input variable data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(maxOp)
  override val reduceCombineOpt = mapCombineOpt
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

class min2 extends DualRDDKernel {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Minimum"
  val description = "Computes element-wise minimum values for a pair of input variables data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(minOp)
}

class max2 extends DualRDDKernel {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val description = "Computes element-wise maximum values for a pair of input variables data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(maxOp)
}

class sum2 extends DualRDDKernel {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val description = "Computes element-wise sums for a pair of input variables data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(addOp)
}

class diff2 extends DualRDDKernel {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Difference"
  val description = "Computes element-wise diffs for a pair of input variables data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(subtractOp)
}

class mult2 extends DualRDDKernel {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Product"
  val description = "Computes element-wise products for a pair of input variables data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(multiplyOp)
}

class div2 extends DualRDDKernel {
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Division"
  val description = "Computes element-wise divisions for a pair of input variables data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(divideOp)
}


class min extends SingularRDDKernel {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Minimum"
  val description = "Computes minimum element value from input variable data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some((x, y) => {
    math.min(x, y)
  })
  override val reduceCombineOpt = mapCombineOpt
  override val initValue: Float = Float.MaxValue

}

class sum extends SingularRDDKernel {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Sum"
  val description = "Computes sums of element values from input variable data over specified axes and roi"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some((x, y) => {
    x + y
  })
  override val reduceCombineOpt = mapCombineOpt
  override val initValue: Float = 0f
}

class average extends SingularRDDKernel {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Mean"
  val description = "Computes (weighted) means of element values from input variable data over specified axes and roi"

  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val elems = context.operation.inputs.map( inputId => inputs.element(inputId) match {
      case Some( input_data ) =>
        val input_array = input_data.toCDFloatArray
        val accumulation_index = input_array.getAccumulationIndex( axes.args )
        val weights: CDFloatArray = KernelUtilities.getWeights(inputId, context)
        val (weighted_value_sum_masked, weights_sum_masked) = input_array.weightedReduce( CDFloatArray.getOp("add"), 0f, accumulation_index, Some(weights) )
        context.operation.rid -> HeapFltArray( weighted_value_sum_masked, input_data.origin, arrayMdata(inputs, "value"), Some(weights_sum_masked) )
      case None => throw new Exception( "Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(",") )
    })
    logger.info("Executed Kernel %s[%d] map op, input = %s, time = %.4f s".format(name, inputs.iPart, id, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( inputs.iPart, Map( elems:_*), inputs.metadata ++ List( "rid" -> context.operation.rid ) )
  }
  override def combineRDD(context: KernelContext)(a0: RDDPartition, a1: RDDPartition, axes: AxisIndices ): RDDPartition =  weightedValueSumRDDCombiner(context)(a0, a1, axes )
  override def postRDDOp( pre_result: RDDPartition, context: KernelContext ):  RDDPartition = weightedValueSumRDDPostOp( pre_result, context )
}

class subset extends Kernel {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Subset"
  val description = "Extracts a subset of element values from input variable data over the specified axes and roi"
}

class timeBin extends Kernel {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Time Binning"
  override val description = "Aggregates data into bins over time using specified reduce function and binning specifications"

  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val period = context.config("period", "1" ).toInt
    val mod = context.config("mod", "12" ).toInt
    val unit = context.config("unit", "month" )
    val offset = context.config("offset", "0" ).toInt
    val async = context.config("async", "false").toBoolean
    val ( id, input_array ) = inputs.head
    val accumulation_index: CDIndexMap = input_array.toCDFloatArray.getIndex.getAccumulator( axes.args, List( getMontlyBinMap( id, context ) )  )
    val (weighted_value_sum_masked, weights_sum_masked) = input_array.toCDFloatArray.weightedReduce( CDFloatArray.getOp("add"), 0f, accumulation_index )
    val elems = Map( context.operation.rid -> HeapFltArray( weighted_value_sum_masked, input_array.origin, arrayMdata(inputs,"value"), None ),
                     "weights"             -> HeapFltArray( weights_sum_masked, input_array.origin, Map.empty[String,String], None ) )
    logger.info("Executed Kernel %s[%d] map op, input = %s, time = %.4f s".format(name, inputs.iPart, id, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( inputs.iPart, elems, inputs.metadata ++ List( "rid" -> context.operation.rid ) )
  }
  override def combineRDD(context: KernelContext)(a0: RDDPartition, a1: RDDPartition, axes: AxisIndices ): RDDPartition =  weightedValueSumRDDCombiner(context)(a0, a1, axes )
  override def postRDDOp( pre_result: RDDPartition, context: KernelContext ):  RDDPartition = weightedValueSumRDDPostOp( pre_result, context )
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
//        logger.info( "Partition[%d], generated anomaly result: %s".format(partIndex, anomaly_result.toDataString ) )
//        val t11 = System.nanoTime
//        DataFragment(resultFragSpec, anomaly_result)
//      } )
//    }
//  }

