package nasa.nccs.cdapi.kernels

import nasa.nccs.cdapi.data.{ArrayBase, HeapFltArray, MetadataOps, RDDPartition}
import nasa.nccs.cdapi.tensors.{CDArray, CDFloatArray}
import nasa.nccs.cdapi.cdm._
import nasa.nccs.esgf.process._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import java.io.{File, IOException, PrintWriter, StringWriter}

import scala.concurrent.ExecutionContext.Implicits.global
import nasa.nccs.caching.collectionDataCache
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import nasa.nccs.utilities.Loggable
import ucar.nc2.Attribute
import ucar.{ma2, nc2}

import scala.util.{Failure, Random, Success}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Port {
  def apply( name: String, cardinality: String, description: String="", datatype: String="", identifier: String="" ) = {
    new Port(  name,  cardinality,  description, datatype,  identifier )
  }
}

class Port( val name: String, val cardinality: String, val description: String, val datatype: String, val identifier: String ) extends Serializable {

  def toXml = {
    <port name={name} cardinality={cardinality}>
      { if ( description.nonEmpty ) <description> {description} </description> }
      { if ( datatype.nonEmpty ) <datatype> {datatype} </datatype> }
      { if ( identifier.nonEmpty ) <identifier> {identifier} </identifier> }
    </port>
  }
}

class KernelContext( val operation: OperationContext, val grid: GridContext, val sectionMap: Map[String,Option[CDSection]], private val configuration: Map[String,String] ) extends Loggable with Serializable with ScopeContext {
  def getConfiguration = configuration ++ operation.getConfiguration
}

class CDASExecutionContext( val operation: OperationContext, val request: RequestContext, val server: ServerContext ) extends Loggable  {

  def getOpSections: Option[ IndexedSeq[ma2.Section] ] = {
    val optargs: Map[String, String] = operation.getConfiguration
    val domains: IndexedSeq[DomainContainer] = optargs.get("domain") match {
      case Some(domainIds) => domainIds.split(",").map(request.getDomain(_))
      case None => return Some( IndexedSeq.empty[ma2.Section] )
    }
//    logger.info( "OPT DOMAIN Arg: " + optargs.getOrElse( "domain", "None" ) )
//    logger.info( "OPT Domains: " + domains.map(_.toString).mkString( ", " ) )
    Some( domains.map(dc => request.targetGrid.grid.getSubSection(dc.axes) match {
      case Some(section) => section
      case None => return None
    }))
  }

  def toKernelContext: KernelContext = {
    val sectionMap: Map[String,Option[CDSection]] = request.inputs.mapValues( _.map( _.cdsection ) ).map(identity)
    new KernelContext( operation, GridContext(request.targetGrid), sectionMap, request.getConfiguration )
  }

  def getOpSectionIntersection: Option[ ma2.Section ] = getOpSections match {
    case None => return None
    case Some( sections ) =>
      if( sections.isEmpty ) None
      else {
        val result = sections.foldLeft(sections.head)( _.intersect(_) )
        if (result.computeSize() > 0) { Some(result) }
        else return None
      }
  }
  def getOpCDSectionIntersection: Option[ CDSection ] = getOpSectionIntersection.map( CDSection( _ ) )
}

class ExecutionResult( val id: String ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def toXml: xml.Elem = <result id={id} > </result>
}

class UtilityExecutionResult( id: String, val response: xml.Elem )  extends ExecutionResult(id) {
  override def toXml = <result id={id}> {response} </result>
}
class BlockingExecutionResult( id: String, val intputSpecs: List[DataFragmentSpec], val gridSpec: TargetGrid, val result_tensor: CDFloatArray, val resultId: Option[String] = None ) extends ExecutionResult(id) {
  override def toXml = {
    val idToks = id.split('-')
    logger.info( "BlockingExecutionResult-> result_tensor(" + id + "): \n" + result_tensor.toString )
    val inputs = intputSpecs.map( _.toXml )
    val grid = gridSpec.toXml
    val results = result_tensor.mkDataString(",")
    <result id={id} op={idToks.head} rid={resultId.getOrElse("")}> { inputs } { grid } <data undefined={result_tensor.getInvalid.toString}> {results}  </data>  </result>
  }
}

class RDDExecutionResult( id: String, val result: RDDPartition,  val resultId: Option[String] = None ) extends ExecutionResult(id) {
  override def toXml = {
    val idToks = id.split('-')
    logger.info( "RDDExecutionResult-> result_tensor(" + id + "): \n" + result.toString )
    <result id={id} op={idToks.head} rid={resultId.getOrElse("")}> { result.toXml } </result>
  }
}


class ErrorExecutionResult( val err: Throwable ) extends ExecutionResult( err.getClass.getName ) {

  def fatal(): String = {
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    err.getMessage
  }

  override def toXml = <error> {fatal()} </error>

}

class XmlExecutionResult( id: String,  val responseXml: xml.Node ) extends ExecutionResult(id) {
  override def toXml = {
    val idToks = id.split('~')
    <result id={idToks(1)} op={idToks(0)}> { responseXml }  </result>
  }
}

class AsyncExecutionResult( id: String )  extends ExecutionResult(id)  {
  def this( resultOpt: Option[String] ) { this( resultOpt.getOrElse("empty") ) }
  override def toXml = { <result id={id} > </result> }
}

class ExecutionResults( val results: List[ExecutionResult] ) {
  def this(err: Throwable ) = this( List( new ErrorExecutionResult( err ) ) )
  def toXml = <results> { results.map(_.toXml) } </results>
}

case class ResultManifest( val name: String, val dataset: String, val description: String, val units: String )

class AxisIndices( private val axisIds: Set[Int] = Set.empty ) {
  def getAxes: Seq[Int] = axisIds.toSeq
  def args = axisIds.toArray
  def includes( axisIndex: Int ): Boolean = axisIds.contains( axisIndex )
  override def toString = axisIds.mkString(",")
}

object Kernel {
  def getResultFile( serverConfiguration: Map[String,String], resultId: String, deleteExisting: Boolean = false ): File = {
    val resultsDirPath = serverConfiguration.getOrElse("wps.results.dir", "~/.wps/results").replace( "~",  System.getProperty("user.home") ).replaceAll("[()]","-").replace("=","~")
    val resultsDir = new File(resultsDirPath); resultsDir.mkdirs()
    val resultFile = new File( resultsDirPath + s"/$resultId.nc" )
    if( deleteExisting && resultFile.exists ) resultFile.delete
    resultFile
  }
}

object pathTest extends App {
  println( System.getProperty("user.home") )
}

object KernelUtilities extends Loggable {
  def getWeights( inputId: String, context: KernelContext ): CDFloatArray =  {
    val weighting_type = context.config("weights", if (context.config("axes", "").contains('y')) "cosine" else "")
    context.sectionMap.get( inputId ).flatten match {
      case Some(section) =>
        weighting_type match {
          case "cosine" =>
            context.grid.getAxisData('y', section) match {
              case Some(axis_data) => computeWeights( weighting_type, Map('y' -> axis_data), section.getShape, Float.MaxValue )
              case None => logger.warn("Can't access AxisData for variable %s => Using constant weighting.".format(inputId)); CDFloatArray.const(section.getShape, 1f)
            }
          case x =>
            if (!x.isEmpty) { logger.warn("Can't recognize weighting method: %s => Using constant weighting.".format(x)) }
            CDFloatArray.const(section.getShape, 1f)
        }
      case None => CDFloatArray.empty
    }
  }

  def computeWeights( weighting_type: String, axisDataMap: Map[ Char, ( Int, ma2.Array ) ], shape: Array[Int], invalid: Float ) : CDFloatArray  = {
    weighting_type match {
      case "cosine" =>
        axisDataMap.get('y') match {
          case Some( ( axisIndex, yAxisData ) ) =>
            val axis_length = yAxisData.getSize
            val axis_data = CDFloatArray.factory( yAxisData, Float.MaxValue )
            assert( axis_length == shape(axisIndex), "Y Axis data mismatch, %d vs %d".format(axis_length,shape(axisIndex) ) )
            val cosineWeights: CDFloatArray = axis_data.map( x => Math.cos( Math.toRadians(x) ).toFloat )
            val base_shape: Array[Int] = Array( (0 until shape.length).map(i => if(i==axisIndex) shape(axisIndex) else 1 ): _* )
            val weightsArray: CDArray[Float] =  CDArray.factory( base_shape, cosineWeights.getStorage, invalid )
            weightsArray.broadcast( shape )
          case None => throw new NoSuchElementException( "Missing axis data in weights computation, type: %s".format( weighting_type ))
        }
      case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
    }
  }
}

abstract class Kernel extends Loggable with Serializable {
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  def operation: String = identifiers.last.toLowerCase
  def module = identifiers.dropRight(1).mkString(".")
  def id   = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")

  val inputs: List[Port]
  val outputs: List[Port]
  val description: String = ""
  val keywords: List[String] = List()
  val identifier: String = ""
  val metadata: String = ""

  val mapCombineOpt: Option[ReduceOpFlt] = None
  val reduceCombineOpt: Option[ReduceOpFlt] = None
  val initValue: Float = 0f

  def getOpName( context: KernelContext ):String = "%s(%s)".format( name, context.operation.inputs.mkString(","))

  def map( partIndex: Int, inputs: List[Option[DataFragment]], context: KernelContext ): Option[DataFragment] = { inputs.head }

  def map( inputs: RDDPartition, context: KernelContext ): RDDPartition = { inputs }


  def combine(context: KernelContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment = reduceCombineOpt match {
    case Some(combineOp) =>
      if (axes.includes(0)) DataFragment(a0.spec, CDFloatArray.combine(combineOp, a0.data, a1.data))
      else { a0 ++ a1 }
    case None => {
      a0 ++ a1
    }
  }

  def combineRDD(context: KernelContext)(rdd0: RDDPartition, rdd1: RDDPartition, axes: AxisIndices): RDDPartition = {
    val new_elements = rdd0.elements.flatMap { case (key, element0) =>
      rdd1.elements.get(key) match {
        case Some(element1) =>
          reduceCombineOpt match {
            case Some(combineOp) =>
              if (axes.includes(0)) Some( key -> element0.combine( combineOp, element1 ) )
              else                  Some( key ->  element0.merge( element1 ) )
            case None =>            Some( key ->  element0.merge( element1 ) )
          }
        case None => None
      }
    }
    RDDPartition( rdd0.iPart, new_elements, rdd0.mergeMetadata( context.operation.name, rdd1 ) )
  }

  def postOp( result: DataFragment, context: KernelContext ):  DataFragment = result

  def postRDDOp( pre_result: RDDPartition, context: KernelContext ):  RDDPartition = pre_result

  def reduceOp(context: KernelContext)(a0op: Option[DataFragment], a1op: Option[DataFragment]): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices(context.config("axes", ""))
    val rv = a0op match {
      case Some(a0) =>
        a1op match {
          case Some(a1) => Some( combine(context)(a0,a1,axes) )
          case None => Some(a0)
        }
      case None =>
        a1op match {
          case Some(a1) => Some(a1)
          case None => None
        }
    }
//    logger.info("Executed %s reduce op, time = %.4f s".format( context.operation.name, (System.nanoTime - t0) / 1.0E9 ) )
    rv
  }

  def reduceRDDOp(context: KernelContext)(a0: RDDPartition, a1:RDDPartition ): RDDPartition = {
    val axes: AxisIndices = context.grid.getAxisIndices(context.config("axes", ""))
    combineRDD(context)(a0,a1,axes)
  }

  def getDataSample (result: CDFloatArray, sample_size: Int = 20): Array[Float] = {
    val result_array = result.floatStorage.array
    val start_value = result_array.size / 3
    result_array.slice (start_value, Math.min (start_value + sample_size, result_array.size) )
  }

  def toXmlHeader =  <kernel module={module} name={name}> { if (description.nonEmpty) <description> {description} </description> } </kernel>

  def toXml = {
    <kernel module={module} name={name}>
      {if (description.nonEmpty) <description>{description}</description> }
      {if (keywords.nonEmpty) <keywords> {keywords.mkString(",")} </keywords> }
      {if (identifier.nonEmpty) <identifier> {identifier} </identifier> }
      {if (metadata.nonEmpty) <metadata> {metadata} </metadata> }
    </kernel>
  }

  def getStringArg( args: Map[String, String], argname: String, defaultVal: Option[String] = None ): String = {
    args.get( argname ) match {
      case Some( sval ) => sval
      case None => defaultVal match { case None => throw new Exception( s"Parameter $argname (int) is reqired for operation " + this.id ); case Some(sval) => sval }
    }
  }

  def getIntArg( args: Map[String, String], argname: String, defaultVal: Option[Int] = None ): Int = {
    args.get( argname ) match {
      case Some( sval ) => try { sval.toInt } catch { case err: NumberFormatException => throw new Exception( s"Parameter $argname must ba an integer: $sval" ) }
      case None => defaultVal match { case None => throw new Exception( s"Parameter $argname (int) is reqired for operation " + this.id ); case Some(ival) => ival }
    }
  }

  def getFloatArg( args: Map[String, String], argname: String, defaultVal: Option[Float] = None ): Float = {
    args.get( argname ) match {
      case Some( sval ) => try { sval.toFloat } catch { case err: NumberFormatException => throw new Exception( s"Parameter $argname must ba a float: $sval" ) }
      case None => defaultVal match { case None => throw new Exception( s"Parameter $argname (float) is reqired for operation " + this.id ); case Some(fval) => fval }
    }
  }

  def weightedValueSumCombiner(context: KernelContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment =  {
    if ( axes.includes(0) ) {
      val vTot: CDFloatArray = a0.data + a1.data
      val wTotOpt: Option[CDFloatArray] = a0.weights.map( w => w + a1.weights.get )
      val dataMap = wTotOpt match { case Some(wTot) => Map("value" -> vTot, "weights" -> wTot) case None =>  Map("value" -> vTot ) }
      logger.info( "weightedValueSumCombiner, values shape = %s, result spec = %s".format( vTot.getShape.mkString(","), a0.spec.toString ) )
      new DataFragment(a0.spec, dataMap, DataFragment.combineCoordMaps(a0, a1) )
    }
    else { a0 ++ a1 }
  }

  def weightedValueSumPostOp( result: DataFragment, context: KernelContext ):  DataFragment = result.weights match {
    case Some( weights_sum ) =>
      logger.info( "weightedValueSumPostOp, values shape = %s, weights shape = %s, result spec = %s".format( result.data.getShape.mkString(","), weights_sum.getShape.mkString(","), result.spec.toString ) )
      new DataFragment( result.spec, Map( "value" -> result.data / weights_sum, "weights"-> weights_sum ), result.optCoordMap )
    case None =>
      result
  }

  def fltArray( a0: RDDPartition, elem: String ): CDFloatArray = a0.element(elem) match { case Some(data) => data.toCDFloatArray; case None => throw new Exception("Error missing array element: " + elem) }
  def optFltArray( a0: RDDPartition, elem: String ): Option[CDFloatArray] = a0.element(elem).map( _.toCDFloatArray )
  def arrayMdata( a0: RDDPartition, elem: String ): Map[String,String] = a0.element(elem) match { case Some(data) => data.metadata; case None => Map.empty }

  def weightedValueSumRDDCombiner(context: KernelContext)(a0: RDDPartition, a1: RDDPartition, axes: AxisIndices ): RDDPartition =  {
    if ( axes.includes(0) ) {
      val vTot: CDFloatArray = fltArray(a0,"value") + fltArray(a1,"value")
      val wTotOpt: Option[CDFloatArray] = optFltArray(a0,"weights").map( w => w + fltArray(a1,"weights") )
      val dataMap = wTotOpt match { case Some(wTot) => Map("value" -> vTot, "weights" -> wTot) case None =>  Map("value" -> vTot ) }
      val array_mdata = MetadataOps.mergeMetadata( context.operation.name, arrayMdata(a0,"value"), arrayMdata(a1,"value") )
      val weights_mdata = MetadataOps.mergeMetadata( context.operation.name, arrayMdata(a0,"weights"), arrayMdata(a1,"weights") )
      val part_mdata = MetadataOps.mergeMetadata( context.operation.name, a0.metadata, a1.metadata )
      logger.info( "weightedValueSumCombiner, values shape = %s, result spec = %s".format( vTot.getShape.mkString(","), a0.metadata.toString ) )
      val elements = List( "value" -> HeapFltArray(vTot, array_mdata ) ) ++ wTotOpt.map( wTot => List( "weights" -> HeapFltArray(wTot, weights_mdata ) ) ).getOrElse( List.empty )
      new RDDPartition( a0.iPart, Map(elements:_*), part_mdata )
    }
    else { a0 ++ a1 }
  }

  def weightedValueSumRDDPostOp( result: RDDPartition, context: KernelContext ):  RDDPartition = optFltArray(result,"weights") match {
    case Some( weights_sum ) =>
      val values = fltArray( result, "value" )
      logger.info( "weightedValueSumPostOp, values shape = %s, weights shape = %s, result spec = %s".format( values.getShape.mkString(","), weights_sum.getShape.mkString(","), result.metadata.toString ) )
      new RDDPartition( result.iPart, Map( "value" -> HeapFltArray( values / weights_sum, arrayMdata(result,"value") ), "weights"-> HeapFltArray( weights_sum, arrayMdata(result,"weights") ) ), result.metadata )
    case None =>
      result
  }
}

//abstract class MultiKernel  extends Kernel {
//  val kernels: List[Kernel]
//
//  def execute( context: CDASExecutionContext, nprocs: Int  ): ExecutionResult = {
//    val inputs: List[PartitionedFragment] = inputVars( context )
//    for( kernel: Kernel <- kernels ) {
//      val result = kernel.mapReduce( inputs, context, nprocs )
//    }
//  }
//}

/*
abstract class DualOperationKernel extends Kernel {

  def mapReduce( inputs: List[PartitionedFragment], context: CDASExecutionContext, nprocs: Int ): Future[Option[DataFragment]] = {
    val future_results1: IndexedSeq[Future[Option[DataFragment]]] = (0 until nprocs).map( iproc => Future { map1(iproc,inputs,context) } )
    reduce1( future_results1, context )
    val future_results2: IndexedSeq[Future[Option[DataFragment]]] = (0 until nprocs).map2( iproc => Future { map(iproc,inputs,context) } )
    reduce2( future_results2, context )
  }
  def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): Option[DataFragment] = {
    val t0 = System.nanoTime
    val inputVar = inputs.head
    val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
    inputVar.domainDataFragment(partIndex).map { (dataFrag) =>
      val async = context.config("async", "false").toBoolean
      val resultFragSpec = dataFrag.getReducedSpec(axes)
      val result_val_masked: CDFloatArray = mapCombineOpt match {
        case Some( combineOp ) => dataFrag.data.reduce( combineOp, axes.args, initValue )
        case None => dataFrag.data
      }
      logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, partIndex, (System.nanoTime - t0) / 1.0E9))
      new DataFragment(resultFragSpec, result_val_masked)
    }
  }
  def weightedValueSumCombiner(context: CDASExecutionContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment =  {
    if ( axes.includes(0) ) {
      val vTot = a0.data + a1.data
      val wTot = a0.optData.map( w => w + a1.optData.get )
      new DataFragment( a0.spec, vTot, wTot )
    }
    else { a0 ++ a1 }
  }

  def weightedValueSumPostOp( future_result: Future[Option[DataFragment]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = {
    future_result.map( _.map( (result: DataFragment) => result.optData match {
      case Some( weights_sum ) => new DataFragment( result.spec, result.data / weights_sum, result.optData )
      case None => result
    } ) )
  }
}
*/
abstract class SingularKernel extends Kernel {
  override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: KernelContext ): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    logger.info("\n\n ****** SingularKernel-reduceOp START, axes = " + axes.getAxes.mkString(",") + "\n")
    inputs.head.map( dataFrag => {
      val async = context.config("async", "false").toBoolean
      val resultFragSpec = dataFrag.getReducedSpec(axes)
      val result_val_masked: CDFloatArray = mapCombineOpt match {
        case Some(combineOp) =>
          val result = dataFrag.data.reduce(combineOp, axes.args, initValue)
          logger.info(" ****** SingularKernel-reduceOp, shape = " + result.getShape.mkString(","))
          result
        case None =>
          logger.info(" ****** SingularKernel-No-Op")
          dataFrag.data
      }
      logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, partIndex, (System.nanoTime - t0) / 1.0E9))
      DataFragment(resultFragSpec, result_val_masked)
    } )
  }
}

abstract class DualKernel extends Kernel {
  override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: KernelContext ): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    assert( inputs.length > 1, "Missing input(s) to dual input operation " + id )
    inputs(0).flatMap( dataFrag0 => {
      inputs(1).map( dataFrag1 => {
        logger.info("DualKernel: %s[%s] + %s[%s]".format( dataFrag0.spec.longname, dataFrag0.data.getShape.mkString(","), dataFrag1.spec.longname, dataFrag1.data.getShape.mkString(",") ) )
        val async = context.config("async", "false").toBoolean
        val result_val_masked: DataFragment = mapCombineOpt match {
          case Some(combineOp) =>
            logger.info( "DIFF2: dataFrag0 coordMap = %s".format( dataFrag0.optCoordMap.map( _.toString ).getOrElse("") ) )
            logger.info( "DIFF2: dataFrag1 coordMap = %s".format( dataFrag1.optCoordMap.map( _.toString ).getOrElse("") ) )
            DataFragment.combine( combineOp, dataFrag0, dataFrag1 )
          case None => dataFrag0
        }
        logger.info("\nExecuted Kernel %s[%d] map op, time = %.4f s".format(name, partIndex, (System.nanoTime - t0) / 1.0E9))
//        logger.info("->> input0(%s): %s".format(dataFrag0.spec.varname, dataFrag0.data.mkDataString(",")))
//        logger.info("->> input1(%s): %s".format(dataFrag1.spec.varname, dataFrag1.data.mkDataString(",")))
//        logger.info("->> result: %s".format(result_val_masked.data.mkDataString(",")))
        result_val_masked
      })
    })
  }
}

abstract class SingularRDDKernel extends Kernel {
  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val result_array_map : Map[String,HeapFltArray] = inputs.elements.mapValues { data  =>
      val input_array = data.toCDFloatArray
      mapCombineOpt match {
        case Some(combineOp) =>
          val result = CDFloatArray(input_array.reduce(combineOp, axes.args, initValue))
          logger.info(" ##### KERNEL [%s]: Map Op: combine, axes = %s, result shape = %s".format( name, axes, result.getShape.mkString(",") ) )
          HeapFltArray( result, data.metadata )
        case None =>
          logger.info(" ##### KERNEL [%s]: Map Op: NONE".format( name ) )
          HeapFltArray( input_array, data.metadata )
      }
    }
    logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, inputs.iPart, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( inputs.iPart, result_array_map )
  }
}

abstract class DualRDDKernel extends Kernel {
  override def map(  inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val input_arrays = context.operation.inputs.flatMap( inputs.element(_) )
    assert( input_arrays.size > 1, "Missing input(s) to dual input operation " + id )
    val result_array: CDFloatArray = mapCombineOpt match {
      case Some(combineOp) => CDFloatArray.combine( combineOp, input_arrays(0).toCDFloatArray, input_arrays(1).toCDFloatArray )
      case None => input_arrays(0).toCDFloatArray
    }
    val result_metadata = input_arrays(0).mergeMetadata( name,input_arrays(1) )
    logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, inputs.iPart, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( inputs.iPart, Map( getOpName(context) -> HeapFltArray(result_array,result_metadata) ), inputs.metadata )
  }
}

class TransientFragment( val dataFrag: DataFragment, val request: RequestContext, val mdata: Map[String,nc2.Attribute] ) extends OperationInput( dataFrag.spec, mdata ) {
  def toXml(id: String): xml.Elem = {
    val units = metadata.get("units") match { case Some(attr) => attr.getStringValue; case None => "" }
    val long_name = metadata.getOrElse("long_name",metadata.getOrElse("fullname",metadata.getOrElse("varname", new Attribute("varname","UNDEF")))).getStringValue
    val description = metadata.get("description") match { case Some(attr) => attr.getStringValue; case None => "" }
    val axes = metadata.get("axes") match { case Some(attr) => attr.getStringValue; case None => "" }
    <result id={id} missing_value={dataFrag.data.getInvalid.toString} shape={dataFrag.data.getShape.mkString("(",",",")")} units={units} long_name={long_name} description={description} axes={axes}> { dataFrag.data.mkBoundedDataString( ", ", 1100 ) } </result>
  }
  def domainDataFragment( partIndex: Int,  optSection: Option[ma2.Section]  ): Option[DataFragment] = Some(dataFrag)
  def data(partIndex: Int ): CDFloatArray = dataFrag.data
  def delete() = {;}
}

//object classTest extends App {
//  import nasa.nccs.cds2.modules.CDS._
//  printf( Kernel.getClass.isAssignableFrom( CDS. ).toString )
//}

