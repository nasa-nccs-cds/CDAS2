package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray._
import nasa.nccs.cdapi.tensors.{CDCoordMap, CDFloatArray, CDTimeCoordMap}
import nasa.nccs.cds2.kernels.KernelTools
import nasa.nccs.esgf.process.{DataFragment, _}
import ucar.ma2
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CDS extends KernelModule with KernelTools {
  override val version = "1.0-SNAPSHOT"
  override val organization = "nasa.nccs"
  override val author = "Thomas Maxwell"
  override val contact = "thomas.maxwell@nasa.gov"

  class max extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Maximum over Axes on Input Fragment"
    override val mapCombineOpt: Option[ReduceOpFlt]  = Some( ( x, y ) => { math.max(x,y) } )
    override val reduceCombineOpt = mapCombineOpt
    override val initValue: Float = -Float.MaxValue
  }

    
  class const extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Sets Input Fragment to constant value"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): Option[DataFragment] = {
      val inputVar: PartitionedFragment = inputs.head
      inputVar.domainDataFragment(partIndex) map { dataFrag  =>
        val axes: AxisIndices = context.request.getAxisIndices (context.operation.config ("axes", "") )
        val async = context.request.config ("async", "false").toBoolean
        val resultFragSpec = dataFrag.getReducedSpec (axes)
        val sval = context.operation.config ("value", "1.0")
        val t10 = System.nanoTime
        val result_val_masked: CDFloatArray = (dataFrag.data := sval.toFloat)
        val t11 = System.nanoTime
        logger.info ("Constant op, time = %.4f s, result sample = %s".format ((t11 - t10) / 1.0E9, getDataSample(result_val_masked).mkString(",").toString) )
        new DataFragment (resultFragSpec, result_val_masked)
      }
    }
  }

  class min extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Minimum over Axes on Input Fragment"
    override val mapCombineOpt: Option[ReduceOpFlt]  = Some( ( x, y ) => { math.min(x,y) } )
    override val reduceCombineOpt = mapCombineOpt
    override val initValue: Float = Float.MaxValue

  }

  class sum extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Sum over Axes on Input Fragment"
    override val mapCombineOpt: Option[ReduceOpFlt]  = Some( ( x, y ) => { x+y } )
    override val reduceCombineOpt = mapCombineOpt
    override val initValue: Float = 0f
  }

  def getDataSample( result: CDFloatArray, sample_size: Int = 20 ): Array[Float] = {
    val result_array = result.floatStorage.array
    val start_value = result_array.size/3
    result_array.slice( start_value,  Math.min( start_value + sample_size, result_array.size ) )
  }

  class average extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Weighted Average over Axes on Input Fragment"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): Option[DataFragment] = {
      val inputVar: PartitionedFragment = inputs.head
      inputVar.domainDataFragment(partIndex) map { dataFrag =>
        val async = context.request.config("async", "false").toBoolean
        val axes: AxisIndices = context.request.getAxisIndices(context.operation.config("axes", ""))
        val resultFragSpec = dataFrag.getReducedSpec(axes)
        val t10 = System.nanoTime
        val weighting_type = context.operation.config("weights", if (context.operation.config("axes", "").contains('y')) "cosine" else "")
        val weights: CDFloatArray = weighting_type match {
          case "cosine" =>
            context.server.getAxisData(inputVar.fragmentSpec, 'y') match {
              case Some(axis_data) => dataFrag.data.computeWeights(weighting_type, Map( 'y' -> axis_data) )
              case None => logger.warn( "Can't access AxisData for variable %s => Using constant weighting.".format(inputVar.fragmentSpec.varname) ); dataFrag.data := 1f
            }
          case x =>
            if( !x.isEmpty ) { logger.warn( "Can't recognize weighting method: %s => Using constant weighting.".format(x) )}
            dataFrag.data := 1f
        }
        val weighted_value_sum_masked: CDFloatArray = ( dataFrag.data * weights ).sum(axes.args)
        val weights_sum_masked: CDFloatArray = weights.sum(axes.args)
        val t11 = System.nanoTime
        logger.info("Mean_val_masked, time = %.4f s, reduction dims = (%s), sample weighted_value_sum = %s".format((t11 - t10) / 1.0E9, axes.args.mkString(","), getDataSample(weighted_value_sum_masked).mkString(",") ))
        new DataFragment(resultFragSpec, weighted_value_sum_masked, Some(weights_sum_masked) )
      }
    }
    override def combine(context: CDASExecutionContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment =  weightedValueSumCombiner(context)(a0, a1, axes )
    override def postOp( future_result: Future[Option[DataFragment]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = weightedValueSumPostOp( future_result, context )

  }

  class subset extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Subset of Input Fragment"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): Option[DataFragment] = {
      val inputVar: PartitionedFragment = inputs.head
      inputVar.domainDataFragment(partIndex) match {
        case Some(dataFrag) =>
          val async = context.request.config("async", "false").toBoolean
          val axes: AxisIndices = context.request.getAxisIndices(context.operation.config("axes", ""))
          val resultFragSpec = dataFrag.getReducedSpec(axes)
          val optargs: Map[String, String] = context.operation.getConfiguration
          optargs.get("domain") match {
            case None => Some(dataFrag)
            case Some(domainId) =>  dataFrag.subset(context.request.targetGrid.grid.getSubSection(context.request.getDomain(domainId).axes))
          }
          case None => None
      }
    }
  }

  class timeBin extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Aggregate data into bins using specified reduce function"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): Option[DataFragment] = {
      val inputVar: PartitionedFragment = inputs.head
      inputVar.domainDataFragment(partIndex) map { dataFrag =>
        val async = context.request.config("async", "false").toBoolean
        val optargs: Map[String, String] = context.operation.getConfiguration
        val axes: AxisIndices = context.request.getAxisIndices(context.operation.config("axes", ""))

        val period = getIntArg(optargs, "period", Some(1) )
        val mod = getIntArg(optargs, "mod",  Some(12) )
        val unit = getStringArg(optargs, "unit",  Some("month") )
        val offset = getIntArg(optargs, "offset", Some(0) )

        val t10 = System.nanoTime
        val cdTimeCoordMap: CDTimeCoordMap = new CDTimeCoordMap(context.request.targetGrid)
        val coordMap: CDCoordMap = cdTimeCoordMap.getTimeCycleMap(period, unit, mod, offset)
        val timeData = cdTimeCoordMap.getTimeIndexIterator("month").toArray
        logger.info("Binned array, timeData = [ %s ]".format(timeData.mkString(",")))
        logger.info("Binned array, coordMap = %s".format(coordMap.toString))
        dataFrag.data.weightedReduce(dataFrag.data.getOp("add"), axes.args, 0f, None, Some(coordMap)) match {
          case (values_sum: CDFloatArray, weights_sum: CDFloatArray) =>
            val t11 = System.nanoTime
            logger.info("Binned array, time = %.4f s, result sample = %s".format((t11 - t10) / 1.0E9, getDataSample(values_sum).mkString(",")))
            val resultFragSpec = dataFrag.getReducedSpec(Set(axes.args(0)), values_sum.getShape(axes.args(0)))
            new DataFragment(resultFragSpec, values_sum, Some(weights_sum) )
        }
      }
    }
    override def combine(context: CDASExecutionContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment =  weightedValueSumCombiner(context)(a0, a1, axes )
    override def postOp( future_result: Future[Option[DataFragment]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = weightedValueSumPostOp( future_result, context )
  }

  class anomaly extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Anomaly over Input Fragment"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): Option[DataFragment] = {
      val inputVar: PartitionedFragment = inputs.head
      inputVar.domainDataFragment(partIndex) map { dataFrag =>
        val async = context.request.config("async", "false").toBoolean
        val axes: AxisIndices = context.request.getAxisIndices(context.operation.config("axes", ""))
        val resultFragSpec = dataFrag.getReducedSpec(axes)
        val t10 = System.nanoTime
        val weighting_type = context.request.config("weights", if (context.operation.config("axis", "").contains('y')) "cosine" else "")
        val weightsOpt: Option[CDFloatArray] = weighting_type match {
          case "" => None
          case wtype => context.server.getAxisData(inputVar.fragmentSpec, 'y').map(axis_data => dataFrag.data.computeWeights(wtype, Map('y' -> axis_data)))
        }
        val anomaly_result: CDFloatArray = dataFrag.data.anomaly(axes.args, weightsOpt)
        logger.info( "Partition[%d], generated anomaly result: shape = [ %s ]".format(partIndex,anomaly_result.getShape.mkString(",") ) )
        val t11 = System.nanoTime
        new DataFragment(resultFragSpec, anomaly_result)
      }
    }
  }
}
