package nasa.nccs.cds2.modules.CDSpark

import nasa.nccs.cdapi.data.{HeapFltArray, RDDPartition}
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray._
import nasa.nccs.cdapi.tensors.{CDCoordMap, CDFloatArray, CDTimeCoordMap}
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.{universe => u}

class max extends SingularRDDKernel {
  val inputs = List(Port("input fragment", "1"))
  val outputs = List(Port("result", "1"))
  override val description = "Maximum over Axes on Input Fragment"
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
  val inputs = List(Port("input fragment", "2"))
  val outputs = List(Port("result", "1"))
  override val description = "Element-wise minimum of two Input Fragments"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(minOp)
}

class max2 extends DualRDDKernel {
  val inputs = List(Port("input fragment", "2"))
  val outputs = List(Port("result", "1"))
  override val description = "Element-wise maximum of two Input Fragments"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(maxOp)
}

class sum2 extends DualRDDKernel {
  val inputs = List(Port("input fragment", "2"))
  val outputs = List(Port("result", "1"))
  override val description = "Element-wise sum of two Input Fragments"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(addOp)
}

class diff2 extends DualRDDKernel {
  val inputs = List(Port("input fragment", "2"))
  val outputs = List(Port("result", "1"))
  override val description = "Element-wise difference of two Input Fragments"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(subtractOp)
}

class mult2 extends DualRDDKernel {
  val inputs = List(Port("input fragment", "2"))
  val outputs = List(Port("result", "1"))
  override val description = "Element-wise multiplication of two Input Fragments"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(multiplyOp)
}

class div2 extends DualRDDKernel {
  val inputs = List(Port("input fragment", "2"))
  val outputs = List(Port("result", "1"))
  override val description = "Element-wise division of two Input Fragments"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some(divideOp)
}


class min extends SingularRDDKernel {
  val inputs = List(Port("input fragment", "1"))
  val outputs = List(Port("result", "1"))
  override val description = "Minimum over Axes on Input Fragment"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some((x, y) => {
    math.min(x, y)
  })
  override val reduceCombineOpt = mapCombineOpt
  override val initValue: Float = Float.MaxValue

}

class sum extends SingularRDDKernel {
  val inputs = List(Port("input fragment", "1"))
  val outputs = List(Port("result", "1"))
  override val description = "Sum over Axes on Input Fragment"
  override val mapCombineOpt: Option[ReduceOpFlt] = Some((x, y) => {
    x + y
  })
  override val reduceCombineOpt = mapCombineOpt
  override val initValue: Float = 0f
}

class average extends SingularRDDKernel {
  val inputs = List(Port("input fragment", "1"))
  val outputs = List(Port("result", "1"))
  override val description = "Weighted Average over Axes on Input Fragment"

  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val ( id, input_array ) = inputs.head
    val weights: CDFloatArray = KernelUtilities.getWeights(id, context)
    val (weighted_value_sum_masked, weights_sum_masked) = input_array.toCDFloatArray.weightedReduce(CDFloatArray.getOp("add"), axes.args, 0f, Some(weights), None)
    val elems = Map( "value" -> HeapFltArray( weighted_value_sum_masked, arrayMdata(inputs,"value") ), "weights" -> HeapFltArray( weights_sum_masked, Map.empty ) )
    logger.info("Executed Kernel %s[%d] map op, input = %s, time = %.4f s".format(name, inputs.iPart, id, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( inputs.iPart, elems, inputs.metadata )
  }
  override def combineRDD(context: KernelContext)(a0: RDDPartition, a1: RDDPartition, axes: AxisIndices ): RDDPartition =  weightedValueSumRDDCombiner(context)(a0, a1, axes )
  override def postRDDOp( pre_result: RDDPartition, context: KernelContext ):  RDDPartition = weightedValueSumRDDPostOp( pre_result, context )
}

class subset extends Kernel {
  val inputs = List(Port("input fragment", "1"))
  val outputs = List(Port("result", "1"))
  override val description = "Subset of Input Fragment"
}

class timeBin extends Kernel {
  val inputs = List(Port("input fragment", "1"))
  val outputs = List(Port("result", "1"))
  override val description = "Aggregate data into bins using specified reduce function"

  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val period = context.config("period", "1" ).toInt
    val mod = context.config("mod", "12" ).toInt
    val unit = context.config("unit", "month" )
    val offset = context.config("offset", "0" ).toInt
    val async = context.config("async", "false").toBoolean
    val ( id, input_array ) = inputs.head
    val coordMap: CDCoordMap = getMontlyBinMap( id, context )
    val (weighted_value_sum_masked, weights_sum_masked) = input_array.toCDFloatArray.weightedReduce(CDFloatArray.getOp("add"), axes.args, 0f, None, Some(coordMap) )
    val elems = Map( "value" -> HeapFltArray( weighted_value_sum_masked, arrayMdata(inputs,"value") ), "weights" -> HeapFltArray( weights_sum_masked, Map.empty ) )
    logger.info("Executed Kernel %s[%d] map op, input = %s, time = %.4f s".format(name, inputs.iPart, id, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( inputs.iPart, elems, inputs.metadata )
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

