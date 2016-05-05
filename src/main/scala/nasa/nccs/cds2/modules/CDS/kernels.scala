package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.{CDCoordArrayMap, CDFloatArray, CDTimeCoordMap}
import nasa.nccs.cds2.kernels.KernelTools
import nasa.nccs.esgf.process._
import ucar.ma2

class CDS extends KernelModule with KernelTools {
  override val version = "1.0-SNAPSHOT"
  override val organization = "nasa.nccs"
  override val author = "Thomas Maxwell"
  override val contact = "thomas.maxwell@nasa.gov"

  class max extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Maximum over Axes on Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput = inputVars(operationCx, requestCx, serverCx).head
      val input_array: CDFloatArray = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val max_val_masked: CDFloatArray = input_array.max( axes.toArray )
      val t11 = System.nanoTime
      logger.info("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, max_val_masked.toString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getReducedSection(Set(axes:_*))
      if(async) {
        new AsyncExecutionResult( saveResult( max_val_masked, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), max_val_masked )
    }
  }

  class const extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Sets Input Fragment to constant value"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput = inputVars(operationCx, requestCx, serverCx).head
      val input_array: CDFloatArray = inputVar.dataFragment.data
      val async = requestCx.config("async", "false").toBoolean
      val sval = operationCx.config("value", "1.0" )
      val t10 = System.nanoTime
      val max_val_masked: CDFloatArray = ( input_array := sval.toFloat )
      val t11 = System.nanoTime
      logger.info("Constant op, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, max_val_masked.toString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getReducedSection(Set())
      if(async) {
        new AsyncExecutionResult( saveResult( max_val_masked, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), max_val_masked )
    }
  }

  class min extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Minimum over Axes on Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput = inputVars(operationCx, requestCx, serverCx).head
      val input_array: CDFloatArray = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val max_val_masked: CDFloatArray = input_array.min( axes.toArray )
      val t11 = System.nanoTime
      logger.info("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, max_val_masked.toString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getReducedSection(Set(axes:_*))
      if(async) {
        new AsyncExecutionResult( saveResult( max_val_masked, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), max_val_masked )
    }
  }

  class sum extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Sum over Axes on Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput = inputVars(operationCx, requestCx, serverCx).head
      val input_array: CDFloatArray = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val max_val_masked: CDFloatArray = input_array.sum( axes.toArray )
      val t11 = System.nanoTime
      logger.info("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, max_val_masked.toString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getReducedSection(Set(axes:_*))
      if(async) {
        new AsyncExecutionResult( saveResult( max_val_masked, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), max_val_masked )
    }
  }

  class average extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Weighted Average over Axes on Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput = inputVars(operationCx, requestCx, serverCx).head
      val optargs: Map[String, String] = operationCx.getConfiguration
      val input_array: CDFloatArray = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val weighting_type = operationCx.config("weights", if( operationCx.config("axes","").contains('y') ) "cosine" else "")
      val weightsOpt: Option[CDFloatArray] = weighting_type match {
        case "" => None
        case "cosine" => Some( input_array.computeWeights( weighting_type, Map( 'y' -> serverCx.getAxisData( inputVar.getSpec, 'y' ) ) ) )
        case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
      }
      val mean_val_masked: CDFloatArray = input_array.mean( axes.toArray, weightsOpt )
      val t11 = System.nanoTime
      logger.info("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, mean_val_masked.toString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getReducedSection(Set(axes:_*))
      if(async) {
        new AsyncExecutionResult( saveResult( mean_val_masked, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), mean_val_masked )
    }
  }
  class subset extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  inputVars( operationCx, requestCx, serverCx ).head
      val optargs: Map[String,String] =  operationCx.getConfiguration
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t0 = System.nanoTime
      def input_uids = requestCx.getDataSources.keySet
      assert( input_uids.size == 1, "Wrong number of arguments to 'subset': %d ".format(input_uids.size) )
      val result: PartitionedFragment = optargs.get("domain") match {
        case None => inputVar.dataFragment
        case Some(domain_id) => serverCx.getSubset( requestCx.getInputSpec(input_uids.head).data, requestCx.getDomain(domain_id) )
      }
      val t1 = System.nanoTime
      logger.info("Subset: time = %.4f s, result = %s".format( (t1-t0)/1.0E9, result.toString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getSubSection(result.fragmentSpec.roi)
      if(async) {
        new AsyncExecutionResult( saveResult( result.data, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), result.data )
    }
  }

  class metadata extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def execute(operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext): ExecutionResult = {
      import nasa.nccs.cds2.loaders.Collections
      val result: (String, xml.Node) = requestCx.inputs.headOption match {
        case None => ("Collection", Collections.toXml)
        case Some((key, inputSpec)) =>
          inputSpec.data.collection match {
            case "" => ("Collection", Collections.toXml)
            case collection =>
              inputSpec.data.varname match {
                case "" => (collection, Collections.toXml(collection))
                case vname => (collection + ":" + vname, serverCx.getVariable(collection, vname).toXml)
              }
          }
      }
      result match {
        case (id, resultXml) => new XmlExecutionResult("Metadata~" + id, resultXml )
      }
    }
  }

  class bin extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Binning over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput = inputVars(operationCx, requestCx, serverCx).head
      val optargs: Map[String, String] = operationCx.getConfiguration
      val input_array: CDFloatArray = inputVar.dataFragment.data
      val cdsVariable = serverCx.getVariable(inputVar.getSpec)
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes.toArray
      val t10 = System.nanoTime
      assert(axes.length == 1, "Must bin over 1 axis only! Requested: " + axes.mkString(","))
      val coordMap: CDCoordArrayMap = CDTimeCoordMap.getTimeCycleMap("month", "year", cdsVariable)
      val binned_array: CDFloatArray = input_array.weightedReduce(input_array.addOp, axes, 0f, None, Some(coordMap)) match {
        case (values_sum: CDFloatArray, weights_sum: CDFloatArray) =>
          values_sum / weights_sum
      }
      val t11 = System.nanoTime
      logger.info("Binned array, time = %.4f s, result = %s".format((t11 - t10) / 1.0E9, binned_array.toString))
      val variable = serverCx.getVariable(inputVar.getSpec)
      val section = inputVar.getSpec.getReducedSection(Set(axes(0)), binned_array.getShape(axes(0)))
      if (async) {
        new AsyncExecutionResult(saveResult(binned_array, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx)))
      }
      else new BlockingExecutionResult(operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), binned_array)
    }
  }

  class anomaly extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Anomaly over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  inputVars( operationCx, requestCx, serverCx ).head
      val optargs: Map[String,String] =  operationCx.getConfiguration
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val weighting_type = requestCx.config("weights", if( operationCx.config("axis","").contains('y') ) "cosine" else "")
      val weightsOpt: Option[CDFloatArray] = weighting_type match {
        case "" => None
        case wtype => Some( input_array.computeWeights( wtype, Map( 'y' -> serverCx.getAxisData( inputVar.getSpec, 'y' ) ) ) )
      }
      val anomaly_result: CDFloatArray = input_array.anomaly( axes.toArray, weightsOpt )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.roi
      val t11 = System.nanoTime
      logger.info("Anomaly, time = %.4f s".format( (t11-t10)/1.0E9 ) )
      if(async) {
        new AsyncExecutionResult( saveResult( anomaly_result, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), anomaly_result )
    }
  }
}
