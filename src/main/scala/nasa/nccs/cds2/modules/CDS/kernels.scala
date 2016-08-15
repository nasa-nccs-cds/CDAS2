package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.CDFloatArray._
import nasa.nccs.cdapi.tensors.{CDCoordMap, CDFloatArray, CDTimeCoordMap}
import nasa.nccs.cds2.kernels.KernelTools
import nasa.nccs.esgf.process.{DataFragment, _}
import ucar.ma2

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
    override val combineOp: ReduceOpFlt  = ( x, y ) => { math.max(x,y) }
    override val initValue: Float = -Float.MaxValue
  }

    
  class const extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Sets Input Fragment to constant value"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): DataFragment = {
      val inputVar: PartitionedFragment = inputs.head
      val dataFrag: DataFragment = inputVar.domainDataFragment(partIndex)
      val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
      val async = context.request.config("async", "false").toBoolean
      val resultFragSpec = dataFrag.getReducedSpec( axes )
      val sval = context.operation.config("value", "1.0" )
      val t10 = System.nanoTime
      val result_val_masked: CDFloatArray = ( dataFrag.data := sval.toFloat )
      val t11 = System.nanoTime
      logger.info("Constant op, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, result_val_masked.toString ) )
      new DataFragment( resultFragSpec, result_val_masked )
    }
  }

  class min extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Minimum over Axes on Input Fragment"
    override val combineOp: ReduceOpFlt  = ( x, y ) => { math.min(x,y) }
    override val initValue: Float = Float.MaxValue

  }

  class sum extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Sum over Axes on Input Fragment"
    override val combineOp: ReduceOpFlt  = ( x, y ) => { x+y }
    override val initValue: Float = 0f
  }

  class average extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Weighted Average over Axes on Input Fragment"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): DataFragment = {
      val inputVar: PartitionedFragment = inputs.head
      val dataFrag: DataFragment = inputVar.domainDataFragment(partIndex)
      val async = context.request.config("async", "false").toBoolean
      val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
      val resultFragSpec = dataFrag.getReducedSpec( axes )
      val t10 = System.nanoTime
      val weighting_type = context.operation.config("weights", if( context.operation.config("axes","").contains('y') ) "cosine" else "")
      val weightsOpt: Option[CDFloatArray] = weighting_type match {
        case "" => None
        case "cosine" => context.server.getAxisData( inputVar.fragmentSpec, 'y' ).map( axis_data => dataFrag.data.computeWeights( weighting_type, Map( 'y' ->  axis_data ) ) )
        case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
      }
      val result_val_masked: CDFloatArray = dataFrag.data.mean( axes.args, weightsOpt )
      val t11 = System.nanoTime
      logger.info("Mean_val_masked, time = %.4f s, reduction dims = (%s), result = %s".format( (t11-t10)/1.0E9, axes.args.mkString(","), result_val_masked.toString ) )
      new DataFragment( resultFragSpec, result_val_masked )
    }
  }

  class subset extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Subset of Input Fragment"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): DataFragment = {
      val inputVar: PartitionedFragment = inputs.head
      val dataFrag: DataFragment = inputVar.domainDataFragment(partIndex)
      val async = context.request.config("async", "false").toBoolean
      val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
      val resultFragSpec = dataFrag.getReducedSpec( axes )
      val optargs: Map[String, String] = context.operation.getConfiguration
      optargs.get("domain") match {
        case None => dataFrag
        case Some( domainId ) => dataFrag.subset( context.request.targetGrid.grid.getSubSection( context.request.getDomain( domainId ).axes ) )
      }
    }
  }

  class timeBin extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Aggregate data into bins using specified reduce function"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): DataFragment = {
      val inputVar: PartitionedFragment = inputs.head
      val dataFrag: DataFragment = inputVar.domainDataFragment(partIndex)
      val async = context.request.config("async", "false").toBoolean
      val optargs: Map[String, String] = context.operation.getConfiguration
      val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )

      val period = getIntArg( optargs, "period", Some(1) )
      val mod = getIntArg( optargs, "mod", Some(Int.MaxValue) )
      val unit = getStringArg( optargs, "unit" )
      val offset = getIntArg( optargs, "offset", Some(0) )

      val t10 = System.nanoTime
      val cdTimeCoordMap: CDTimeCoordMap = new CDTimeCoordMap(context.request.targetGrid)
      val coordMap: CDCoordMap = cdTimeCoordMap.getTimeCycleMap( period, unit, mod, offset )
      val timeData  = cdTimeCoordMap.getTimeIndexIterator( "month" ).toArray
      logger.info( "Binned array, timeData = [ %s ]".format( timeData.mkString(",") ) )
      logger.info( "Binned array, coordMap = %s".format( coordMap.toString ) )
      val binned_array: CDFloatArray = dataFrag.data.weightedReduce( dataFrag.data.getOp("add"), axes.args, 0f, None, Some(coordMap)) match {
        case (values_sum: CDFloatArray, weights_sum: CDFloatArray) =>
          values_sum / weights_sum
      }
      val t11 = System.nanoTime
      logger.info("Binned array, time = %.4f s, result = %s".format((t11 - t10) / 1.0E9, binned_array.toString))
      val resultFragSpec = dataFrag.getReducedSpec( Set(axes.args(0)), binned_array.getShape(axes.args(0)) )
      new DataFragment( resultFragSpec, binned_array )
    }
  }

  class anomaly extends SingularKernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Anomaly over Input Fragment"

    override def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): DataFragment = {
      val inputVar: PartitionedFragment = inputs.head
      val dataFrag: DataFragment = inputVar.domainDataFragment(partIndex)
      val async = context.request.config("async", "false").toBoolean
      val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
      val resultFragSpec = dataFrag.getReducedSpec( axes )
      val t10 = System.nanoTime
      val weighting_type = context.request.config("weights", if( context.operation.config("axis","").contains('y') ) "cosine" else "")
      val weightsOpt: Option[CDFloatArray] = weighting_type match {
        case "" => None
        case wtype => context.server.getAxisData( inputVar.fragmentSpec, 'y' ).map( axis_data => dataFrag.data.computeWeights( wtype, Map( 'y' ->  axis_data ) ) )
      }
      val anomaly_result: CDFloatArray = dataFrag.data.anomaly( axes.args, weightsOpt )
      val t11 = System.nanoTime
      new DataFragment( resultFragSpec, anomaly_result )    }
  }
}
