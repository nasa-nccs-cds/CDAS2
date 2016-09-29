package nasa.nccs.cds2.modules.CDSpark

import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray._
import nasa.nccs.cdapi.tensors.{CDCoordMap, CDFloatArray, CDTimeCoordMap}
import nasa.nccs.cds2.kernels.KernelTools
import nasa.nccs.esgf.process.DataFragment
import nasa.nccs.utilities.cdsutils

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

//  class average extends SingularRDDKernel {
//    val inputs = List(Port("input fragment", "1"))
//    val outputs = List(Port("result", "1"))
//    override val description = "Weighted Average over Axes on Input Fragment"
//
//    override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: OperationContext ): Option[DataFragment] = {
//      inputs.head.map( dataFrag => {
//        val async = context.config("async", "false").toBoolean
//        val axes: AxisIndices = context.getAxisIndices(context.config("axes", ""))
//        val resultFragSpec = dataFrag.getReducedSpec(axes)
//        val t10 = System.nanoTime
//        val weighting_type = context.config("weights", if (context.config("axes", "").contains('y')) "cosine" else "")
//        val weights: CDFloatArray = weighting_type match {
//          case "cosine" =>
//            context.targetGrid.getAxisData( 'y', dataFrag.spec.roi ) match {
//              case Some(axis_data) => dataFrag.data.computeWeights(weighting_type, Map( 'y' -> axis_data) )
//              case None => logger.warn( "Can't access AxisData for variable %s => Using constant weighting.".format(dataFrag.spec.varname) ); dataFrag.data := 1f
//            }
//          case x =>
//            if( !x.isEmpty ) { logger.warn( "Can't recognize weighting method: %s => Using constant weighting.".format(x) )}
//            dataFrag.data := 1f
//        }
//        val ( weighted_value_sum_masked, weights_sum_masked ) =  dataFrag.data.weightedReduce(CDFloatArray.getOp("add"), axes.args, 0f, Some(weights), None )
//        val t11 = System.nanoTime
//        logger.info("Mean_val_masked, time = %.4f s, reduction dims = (%s), sample weighted_value_sum = %s".format((t11 - t10) / 1.0E9, axes.args.mkString(","), getDataSample(weighted_value_sum_masked).mkString(",") ))
//        DataFragment(resultFragSpec, weighted_value_sum_masked, weights_sum_masked )
//      } )
//    }
//    override def combine(context: OperationContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment =  weightedValueSumCombiner(context)(a0, a1, axes )
//    override def postOp( result: DataFragment, context: OperationContext ):  DataFragment = weightedValueSumPostOp( result, context )
//
//  }

class subset extends Kernel {
  val inputs = List(Port("input fragment", "1"))
  val outputs = List(Port("result", "1"))
  override val description = "Subset of Input Fragment"
}

//  class timeBin extends Kernel {
//    val inputs = List(Port("input fragment", "1"))
//    val outputs = List(Port("result", "1"))
//    override val description = "Aggregate data into bins using specified reduce function"
//
//    override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: OperationContext ): Option[DataFragment] = {
//      inputs.head.map( dataFrag => {
//        val async = context.config("async", "false").toBoolean
//        val optargs: Map[String, String] = context.getConfiguration
//        val axes: AxisIndices = context.getAxisIndices(context.config("axes", ""))
//
//        val period = getIntArg(optargs, "period", Some(1) )
//        val mod = getIntArg(optargs, "mod",  Some(12) )
//        val unit = getStringArg(optargs, "unit",  Some("month") )
//        val offset = getIntArg(optargs, "offset", Some(0) )
//        logger.info("timeBin, input shape = [ %s ], roi = [ %s ]".format(dataFrag.data.getShape.mkString(","),dataFrag.spec.roi.toString))
//
//        val t10 = System.nanoTime
//        val cdTimeCoordMap: CDTimeCoordMap = new CDTimeCoordMap( context.targetGrid, dataFrag.spec.roi )
//        val coordMap: CDCoordMap = cdTimeCoordMap.getMontlyBinMap( dataFrag.spec.roi )
//        //  val coordMap: CDCoordMap = cdTimeCoordMap.getTimeCycleMap(period, unit, mod, offset)
//        val timeData = cdTimeCoordMap.getTimeIndexIterator( "month", dataFrag.spec.roi.getRange(0) ).toArray
//        //        logger.info("Binned array, timeData = [ %s ]".format(timeData.mkString(",")))
//        //        logger.info("Binned array, coordMap = %s".format(coordMap.toString))
//        //        logger.info("Binned array, dates = %s".format(cdTimeCoordMap.getDates.mkString(", ")))
//        //        logger.info("Binned array, input data = %s".format(dataFrag.data.toDataString))
//        dataFrag.data.weightedReduce(CDFloatArray.getOp("add"), axes.args, 0f, None, Some(coordMap)) match {
//          case (values_sum: CDFloatArray, weights_sum: CDFloatArray) =>
//            val t11 = System.nanoTime
//            //            logger.info("Binned array, time = %.4f s, section = %s\n *** values = %s\n *** weights=%s".format((t11 - t10) / 1.0E9, dataFrag.spec.roi.toString, values_sum.toDataString, weights_sum.toDataString ))
//            //            val resultFragSpec = dataFrag.getReducedSpec(Set(axes.args(0)), values_sum.getShape(axes.args(0)))
//            logger.info("timeBin, result shape = [ %s ], result spec = %s".format(values_sum.getShape.mkString(","),dataFrag.spec.toString))
//            DataFragment( dataFrag.spec, values_sum, weights_sum, coordMap )
//        }
//      })
//    }
//    override def combine(context: OperationContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment =  weightedValueSumCombiner(context)(a0, a1, axes )
//    override def postOp( result: DataFragment, context: OperationContext ):  DataFragment = weightedValueSumPostOp( result, context )
//  }


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

