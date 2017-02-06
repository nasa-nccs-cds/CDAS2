package nasa.nccs.cdas.kernels

import java.io._

import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data.{HeapFltArray, _}
import nasa.nccs.cdapi.tensors.CDFloatArray.{ReduceNOpFlt, ReduceOpFlt, ReduceWNOpFlt}
import nasa.nccs.cdapi.tensors.{CDArray, CDCoordMap, CDFloatArray, CDTimeCoordMap}
import nasa.nccs.cdas.workers.TransVar
import nasa.nccs.cdas.workers.python.{PythonWorker, PythonWorkerPortal}
import nasa.nccs.cdas.utilities.appParameters
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.Loggable
import nasa.nccs.wps.{WPSProcess, WPSProcessOutput}
import ucar.nc2.Attribute
import ucar.{ma2, nc2}

import scala.collection.GenTraversableOnce
import scala.collection.JavaConversions._

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

class KernelContext( val operation: OperationContext, val grids: Map[String,Option[GridContext]], val sectionMap: Map[String,Option[CDSection]],  val domains: Map[String,DomainContainer],  _configuration: Map[String,String] ) extends Loggable with Serializable with ScopeContext {
  val crsOpt = getCRS
  val trsOpt = getTRS
  val configuration = crsOpt.map( crs => _configuration + ("crs" -> crs ) ) getOrElse( _configuration )
  lazy val grid: GridContext = getTargetGridContext
  def findGrid( varUid: String ): Option[GridContext] = grids.find( item => item._1.split('-')(0).equals(varUid) ).flatMap( _._2 )
  def getConfiguration = configuration ++ operation.getConfiguration
  def getAxes: AxisIndices = grid.getAxisIndices( config("axes", "") )
  def getContextStr = getConfiguration map { case ( key, value ) => key + ":" + value } mkString ";"
  def getDomainMetadata(domId: String): Map[String,String] = domains.get(domId) match { case Some(dc) => dc.metadata; case None => Map.empty }
  def findAnyGrid: GridContext = (grids.find { case (k, v) => v.isDefined }).getOrElse(("", None))._2.getOrElse(throw new Exception("Undefined grid in KernelContext for op " + operation.identifier))
  private def getCRS: Option[String] = operation.getDomain flatMap ( domId => domains.get( domId ).flatMap ( dc => dc.metadata.get("crs") ) )
  private def getTRS: Option[String] = operation.getDomain flatMap ( domId => domains.get( domId ).flatMap ( dc => dc.metadata.get("trs") ) )
  def conf( params: Map[String,String] ): KernelContext = new KernelContext( operation, grids, sectionMap, domains, configuration ++ params )

  private def getTargetGridContext: GridContext = crsOpt match {
    case Some( crs ) =>
      if( crs.startsWith("~") ) { findGrid( crs.substring(1) ).getOrElse( throw new Exception(s"Unsupported grid specification '$crs' in KernelContext for op '$operation'" ) ) }
      else if( crs.contains('~') ) { findAnyGrid }
      else { throw new Exception( "Currently unsupported crs specification") }
    case None => findAnyGrid
  }

//  def getGridSection( inputId: String ): Option[GridSection] = sectionMap.getOrElse(None).map( section => GridSection())
}

//class CDASExecutionContext( val operation: OperationContext, val request: RequestContext, val server: ServerContext ) extends Loggable  {
//  val targetGrids = request.getTargetGrids
//
//  def getOpSections(uid: String): Option[ IndexedSeq[ma2.Section] ] = {
//    val optargs: Map[String, String] = operation.getConfiguration
//    val domains: IndexedSeq[DomainContainer] = optargs.get("domain") match {
//      case Some(domainIds) => domainIds.split(",").map(request.getDomain(_))
//      case None => return Some( IndexedSeq.empty[ma2.Section] )
//    }
////    logger.info( "OPT DOMAIN Arg: " + optargs.getOrElse( "domain", "None" ) )
////    logger.info( "OPT Domains: " + domains.map(_.toString).mkString( ", " ) )
//    targetGrids.get(uid).flatMap( _.map ( targetGrid => domains.map( dc => targetGrid.grid.getSubSection(dc.axes).getOrElse(new ma2.Section(List.empty)) ) ) )
//  }
//
//  def toKernelContext: KernelContext = {
//    val sectionMap: Map[String,Option[CDSection]] = request.inputs.mapValues( _.map( _.cdsection ) ).map(identity)
//    new KernelContext( operation, targetGrids.mapValues(_.map(GridContext(_))), sectionMap, request.getConfiguration )
//  }
//
//  def getOpSectionIntersection(uid: String): Option[ ma2.Section ] = getOpSections(uid) match {
//    case None => return None
//    case Some( sections ) =>
//      if( sections.isEmpty ) None
//      else {
//        val result = sections.foldLeft(sections.head)( _.intersect(_) )
//        if (result.computeSize() > 0) { Some(result) }
//        else return None
//      }
//  }
//  def getOpCDSectionIntersection: Option[ CDSection ] = getOpSectionIntersection().map( CDSection( _ ) )
//}

case class ResultManifest( val name: String, val dataset: String, val description: String, val units: String )

class AxisIndices( private val axisIds: Set[Int] = Set.empty ) {
  def getAxes: Seq[Int] = axisIds.toSeq
  def args = axisIds.toArray
  def includes( axisIndex: Int ): Boolean = axisIds.contains( axisIndex )
  override def toString = axisIds.mkString(",")
}

object Kernel {
  val customKernels = List[Kernel]( new CDMSRegridKernel() )
  def getResultFile( resultId: String, deleteExisting: Boolean = false ): File = {
    val resultsDirPath = appParameters("wps.results.dir", "~/.wps/results").replace( "~",  System.getProperty("user.home") ).replaceAll("[()]","-").replace("=","~")
    val resultsDir = new File(resultsDirPath); resultsDir.mkdirs()
    val resultFile = new File( resultsDirPath + s"/$resultId.nc" )
    if( deleteExisting && resultFile.exists ) resultFile.delete
    resultFile
  }

  def apply(module: String, kernelSpec: String, api: String): Kernel = {
    val specToks = kernelSpec.split("[;]")
    customKernels.find(_.matchesSpecs(specToks)) match {
      case Some(kernel) => kernel
      case None => api match {
        case "python" => new zmqPythonKernel(module, specToks(0), specToks(1), specToks(2), str2Map(specToks(3)))
      }
      case wtf => throw new Exception("Unrecognized kernel api: " + api)
    }
  }

  private def str2Map( metadata: String ): Map[String,String] =
    Map( metadata.stripPrefix("{").stripSuffix("}").split("[,]").toSeq map { pair => pair.split("[:]") } map { a => ( a(0).replaceAll("[\"' ]",""), a(1).replaceAll("[\"' ]","") ) }: _* )

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
            val weightsArray: CDArray[Float] =  CDArray( base_shape, cosineWeights.getStorage, invalid )
            weightsArray.broadcast( shape )
          case None => throw new NoSuchElementException( "Missing axis data in weights computation, type: %s".format( weighting_type ))
        }
      case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
    }
  }
}

class KIType { val Op = 0; val MData = 1 }

abstract class Kernel( val options: Map[String,String] ) extends Loggable with Serializable with WPSProcess {
  val identifiers = this.getClass.getName.split('$').flatMap(_.split('.'))
  def operation: String = identifiers.last.toLowerCase
  def module: String = identifiers.dropRight(1).mkString(".")
  def id = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")
  def parallelizable: Boolean = options.getOrElse("parallelizable","true").toBoolean
  val identifier = name
  def matchesSpecs( specs: Array[String] ): Boolean = { (specs.size >= 2) && specs(0).equals(module) && specs(1).equals(operation) }

  val mapCombineOp: Option[ReduceOpFlt] = options.get("mapOp").fold (options.get("mapreduceOp")) (Some(_)) map ( CDFloatArray.getOp(_) )
  val mapCombineNOp: Option[ReduceNOpFlt] = None
  val mapCombineWNOp: Option[ReduceWNOpFlt] = None
  val reduceCombineOp: Option[ReduceOpFlt] = options.get("reduceOp").fold (options.get("mapreduceOp")) (Some(_)) map ( CDFloatArray.getOp(_) )
  val initValue: Float = 0f
  def cleanUp() = {}

  def getOpName(context: KernelContext): String = "%s(%s)".format(name, context.operation.inputs.mkString(","))
  def map(partIndex: Int, inputs: List[Option[DataFragment]], context: KernelContext): Option[DataFragment] = inputs.head
  def map( rdd: RDDPartition, context: KernelContext ): RDDPartition = rdd // , inputs: Map[String,KIType]

  def combine(context: KernelContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices): DataFragment = reduceCombineOp match {
    case Some(combineOp) =>
      if (axes.includes(0)) DataFragment(a0.spec, CDFloatArray.combine(combineOp, a0.data, a1.data))
      else {
        a0 ++ a1
      }
    case None => {
      a0 ++ a1
    }
  }

  def getCombinedGridfile( inputs: Map[String,ArrayBase[Float]] ): String = {
    for ( ( id, array ) <- inputs ) array.metadata.get("gridfile") match { case Some(gridfile) => return gridfile; case None => Unit }
    throw new Exception( " Missing gridfile in kernel inputs: " + name )
  }

  def combineRDD(context: KernelContext)(rdd0: RDDPartition, rdd1: RDDPartition, axes: AxisIndices): RDDPartition = {
    val t0 = System.nanoTime
//    logger.info("&COMBINE: start OP %s (%d <-> %d)".format( context.operation.name, rdd0.iPart, rdd1.iPart  ) )
    val ascending = rdd0.iPart < rdd1.iPart
    val new_elements = rdd0.elements.flatMap { case (key, element0) =>
      rdd1.elements.get(key) match {
        case Some(element1) =>
          reduceCombineOp match {
            case Some(combineOp) =>
              if (axes.includes(0)) Some(key -> element0.combine(combineOp, element1))
              else Some(key -> { if(ascending) element0.append(element1) else element1.append(element0) } )
            case None => Some(key -> { if(ascending) element0.append(element1) else element1.append(element0) } )
          }
        case None => None
      }
    }
    logger.info("&COMBINE: finish OP %s (%d <-> %d), time = %.4f s".format( context.operation.name, rdd0.iPart, rdd1.iPart, (System.nanoTime - t0) / 1.0E9 ) )
    RDDPartition(rdd0.iPart, new_elements, rdd0.mergeMetadata(context.operation.name, rdd1))
  }

  def customReduceRDD(context: KernelContext)( a0: ( Int, RDDPartition ), a1: ( Int, RDDPartition ) ): ( Int, RDDPartition ) = {
    logger.warn( s"No reducer defined for parallel op '$name', executing simple merge." )
    mergeRDD(context)( a0, a1 )
  }

  def mergeRDD(context: KernelContext)( a0: ( Int, RDDPartition ), a1: ( Int, RDDPartition ) ): ( Int, RDDPartition ) = {
    val ( rdd0, rdd1 ) = ( a0._2, a1._2 )
    val t0 = System.nanoTime
    logger.info("&MERGE: start (%d <-> %d)".format( rdd0.iPart, rdd1.iPart  ) )
    val ascending = rdd0.iPart < rdd1.iPart
    val new_elements = rdd0.elements.flatMap {
      case (key, element0) =>  rdd1.elements.get(key).map( element1 => key -> { if(ascending) element0.append(element1) else element1.append(element0) } )
    }
    logger.info("&MERGE: complete in time = %.4f s".format( (System.nanoTime - t0) / 1.0E9 ) )
    a0._1 -> RDDPartition( rdd0.iPart, new_elements, rdd0.mergeMetadata("merge", rdd1) )
  }

  def postOp(result: DataFragment, context: KernelContext): DataFragment = result

  def postRDDOp(pre_result: RDDPartition, context: KernelContext): RDDPartition = pre_result

  def reduceOp(context: KernelContext)(a0op: Option[DataFragment], a1op: Option[DataFragment]): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices(context.config("axes", ""))
    val rv = a0op match {
      case Some(a0) =>
        a1op match {
          case Some(a1) => Some(combine(context)(a0, a1, axes))
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

  def reduceRDDOp(context: KernelContext)(a0: ( Int, RDDPartition ), a1: ( Int, RDDPartition ) ): ( Int, RDDPartition ) = {
    val axes: AxisIndices = context.getAxes
    a0._1 -> combineRDD(context)( a0._2, a1._2, axes )
  }

  def getDataSample(result: CDFloatArray, sample_size: Int = 20): Array[Float] = {
    val result_array = result.floatStorage.array
    val start_value = result_array.size / 3
    result_array.slice(start_value, Math.min(start_value + sample_size, result_array.size))
  }

  def toXmlHeader = <kernel module={module} name={name}>
    {if (title.nonEmpty) <title> {title} </title>}
    {if (description.nonEmpty) <description> {description} </description>}
  </kernel>

  def getStringArg(args: Map[String, String], argname: String, defaultVal: Option[String] = None): String = {
    args.get(argname) match {
      case Some(sval) => sval
      case None => defaultVal match {
        case None => throw new Exception(s"Parameter $argname (int) is reqired for operation " + this.id);
        case Some(sval) => sval
      }
    }
  }

  def getIntArg(args: Map[String, String], argname: String, defaultVal: Option[Int] = None): Int = {
    args.get(argname) match {
      case Some(sval) => try {
        sval.toInt
      } catch {
        case err: NumberFormatException => throw new Exception(s"Parameter $argname must ba an integer: $sval")
      }
      case None => defaultVal match {
        case None => throw new Exception(s"Parameter $argname (int) is reqired for operation " + this.id);
        case Some(ival) => ival
      }
    }
  }

  def getFloatArg(args: Map[String, String], argname: String, defaultVal: Option[Float] = None): Float = {
    args.get(argname) match {
      case Some(sval) => try {
        sval.toFloat
      } catch {
        case err: NumberFormatException => throw new Exception(s"Parameter $argname must ba a float: $sval")
      }
      case None => defaultVal match {
        case None => throw new Exception(s"Parameter $argname (float) is reqired for operation " + this.id);
        case Some(fval) => fval
      }
    }
  }

//  def weightedValueSumCombiner(context: KernelContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices): DataFragment = {
//    if (axes.includes(0)) {
//      val vTot: CDFloatArray = a0.data + a1.data
//      val wTotOpt: Option[CDFloatArray] = a0.weights.map(w => w + a1.weights.get)
//      val dataMap = wTotOpt match {
//        case Some(wTot) => Map("value" -> vTot, "weights" -> wTot)
//        case None => Map("value" -> vTot)
//      }
//      logger.info("weightedValueSumCombiner, values shape = %s, result spec = %s".format(vTot.getShape.mkString(","), a0.spec.toString))
//      new DataFragment(a0.spec, dataMap, DataFragment.combineCoordMaps(a0, a1))
//    }
//    else {
//      a0 ++ a1
//    }
//  }
//
//  def weightedValueSumPostOp(result: DataFragment, context: KernelContext): DataFragment = result.weights match {
//    case Some(weights_sum) =>
//      logger.info("weightedValueSumPostOp, values shape = %s, weights shape = %s, result spec = %s".format(result.data.getShape.mkString(","), weights_sum.getShape.mkString(","), result.spec.toString))
//      new DataFragment(result.spec, Map("value" -> result.data / weights_sum, "weights" -> weights_sum), result.optCoordMap)
//    case None =>
//      result
//  }

  def fltArray(a0: RDDPartition, elem: String): CDFloatArray = a0.element(elem) match {
    case Some(data) => data.toCDFloatArray;
    case None => throw new Exception("Error missing array element: " + elem)
  }
  def optFltArray(a0: RDDPartition, elem: String): Option[CDFloatArray] = a0.element(elem).map(_.toCDFloatArray)

  def wtArray(a0: RDDPartition, elem: String): Option[CDFloatArray] = a0.element(elem).flatMap( _.toCDWeightsArray )

  def originArray(a0: RDDPartition, elem: String): Array[Int]  = a0.element(elem) match {
    case Some(data) => data.origin;
    case None => throw new Exception("Error missing array element: " + elem)
  }

  def arrayMdata(a0: RDDPartition, elem: String): Map[String, String] = a0.element(elem) match {
    case Some(data) => data.metadata;
    case None => Map.empty
  }

  def weightedValueSumRDDCombiner(context: KernelContext)(a0: RDDPartition, a1: RDDPartition, axes: AxisIndices): RDDPartition = {
    if (axes.includes(0)) {
      val rid = context.operation.rid
      val vTot: CDFloatArray = fltArray(a0, rid) + fltArray(a1, rid)
      val vOrigin: Array[Int] = originArray( a0, rid )
      val wTotOpt: Option[Array[Float]] = wtArray(a0, rid ).map(w => w + wtArray(a1,rid).get ).map(_.getArrayData())
      val array_mdata = MetadataOps.mergeMetadata( context.operation.name )( arrayMdata(a0, rid), arrayMdata(a1, rid) )
      val element = rid -> HeapFltArray( vTot, vOrigin, array_mdata, wTotOpt )
      val part_mdata = MetadataOps.mergeMetadata( context.operation.name )( a0.metadata, a1.metadata )
      logger.info("weightedValueSumCombiner, values shape = %s, result spec = %s".format(vTot.getShape.mkString(","), a0.metadata.toString))
      new RDDPartition(a0.iPart, Map(element), part_mdata)
    }
    else {
      a0 ++ a1
    }
  }

  def weightedValueSumRDDPostOp(result: RDDPartition, context: KernelContext): RDDPartition = {
    val rid = context.operation.rid
    wtArray( result, rid ) match {
      case Some(weights_sum) =>
        val values = fltArray(result, rid)
        val vOrigin: Array[Int] = originArray(result, rid)
        logger.info("weightedValueSumPostOp, values shape = %s, weights shape = %s, result spec = %s".format(values.getShape.mkString(","), weights_sum.getShape.mkString(","), result.metadata.toString))
        new RDDPartition( result.iPart, Map(rid -> HeapFltArray( values / weights_sum, vOrigin, arrayMdata(result, "value"), Some( weights_sum.getArrayData() ) ) ), result.metadata )
      case None =>
        result
    }
  }

  def getMontlyBinMap(id: String, context: KernelContext): CDCoordMap = {
    context.sectionMap.get(id).flatten.map( _.toSection ) match  {
      case Some( section ) =>
        val cdTimeCoordMap: CDTimeCoordMap = new CDTimeCoordMap( context.grid, section )
        cdTimeCoordMap.getMontlyBinMap( section )
      case None => throw new Exception( "Error, can't get section for input " + id )
    }
  }

}

//abstract class MultiKernel  extends Kernel {
//  val kernels: List[Kernel]
//
//  def execute( context: CDASExecutionContext, nprocs: Int  ): WPSResponse = {
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
//abstract class SingularKernel extends Kernel {
//  override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: KernelContext ): Option[DataFragment] = {
//    val t0 = System.nanoTime
//    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
//    logger.info("\n\n ****** SingularKernel-reduceOp START, axes = " + axes.getAxes.mkString(",") + "\n")
//    inputs.head.map( dataFrag => {
//      val async = context.config("async", "false").toBoolean
//      val resultFragSpec = dataFrag.getReducedSpec(axes)
//      val result_val_masked: CDFloatArray = mapCombineOpt match {
//        case Some(combineOp) =>
//          val result = dataFrag.data.reduce(combineOp, axes.args, initValue)
//          logger.info(" ****** SingularKernel-reduceOp, shape = " + result.getShape.mkString(","))
//          result
//        case None =>
//          logger.info(" ****** SingularKernel-No-Op")
//          dataFrag.data
//      }
//      logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, partIndex, (System.nanoTime - t0) / 1.0E9))
//      DataFragment(resultFragSpec, result_val_masked)
//    } )
//  }
//}
//
//abstract class DualKernel extends Kernel {
//  override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: KernelContext ): Option[DataFragment] = {
//    val t0 = System.nanoTime
//    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
//    assert( inputs.length > 1, "Missing input(s) to dual input operation " + id )
//    inputs(0).flatMap( dataFrag0 => {
//      inputs(1).map( dataFrag1 => {
//        logger.info("DualKernel: %s[%s] + %s[%s]".format( dataFrag0.spec.longname, dataFrag0.data.getShape.mkString(","), dataFrag1.spec.longname, dataFrag1.data.getShape.mkString(",") ) )
//        val async = context.config("async", "false").toBoolean
//        val result_val_masked: DataFragment = mapCombineOpt match {
//          case Some(combineOp) =>
//            logger.info( "DIFF2: dataFrag0 coordMap = %s".format( dataFrag0.optCoordMap.map( _.toString ).getOrElse("") ) )
//            logger.info( "DIFF2: dataFrag1 coordMap = %s".format( dataFrag1.optCoordMap.map( _.toString ).getOrElse("") ) )
//            DataFragment.combine( combineOp, dataFrag0, dataFrag1 )
//          case None => dataFrag0
//        }
//        logger.info("\nExecuted Kernel %s[%d] map op, time = %.4f s".format(name, partIndex, (System.nanoTime - t0) / 1.0E9))
////        logger.info("->> input0(%s): %s".format(dataFrag0.spec.varname, dataFrag0.data.mkDataString(",")))
////        logger.info("->> input1(%s): %s".format(dataFrag1.spec.varname, dataFrag1.data.mkDataString(",")))
////        logger.info("->> result: %s".format(result_val_masked.data.mkDataString(",")))
//        result_val_masked
//      })
//    })
//  }
//}

abstract class SingularRDDKernel( options: Map[String,String] ) extends Kernel(options)  {
  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val key = inputs.iPart
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val inputId = context.operation.inputs.headOption.getOrElse("NULL")
    if( key == 0 ) {
      val shape = inputs.elements.head._2.shape
      logger.info(" ##### KERNEL [%s]: Map Op: combine, part = 0, input shape = %s".format( name, shape.mkString(",") ) )
    }
    val elem = inputs.findElements(inputId).headOption match {
      case Some( input_array ) =>
        mapCombineOp match {
          case Some(combineOp) =>
            val result = CDFloatArray(input_array.toCDFloatArray.reduce(combineOp, axes.args, initValue))
            logger.info(" ##### KERNEL [%s]: Map Op: combine, axes = %s, result shape = %s".format( name, axes, result.getShape.mkString(",") ) )
            context.operation.rid -> HeapFltArray( result, input_array.origin, input_array.metadata, None )
          case None =>
            logger.info(" ##### KERNEL [%s]: Map Op: NONE".format( name ) )
            context.operation.rid -> HeapFltArray( input_array.toCDFloatArray, input_array.origin, input_array.metadata, None )
        }
      case None => throw new Exception( "Missing input to '" + this.getClass.getName + "' map op: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(",") )
    }
    logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, inputs.iPart, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( key, Map( elem ) )
  }
}

abstract class DualRDDKernel( options: Map[String,String] ) extends Kernel(options)  {
  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val key = inputs.iPart
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val input_arrays: List[ArrayBase[Float]] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[ArrayBase[Float]]())( _ ++ _ )
    assert( input_arrays.size > 1, "Missing input(s) to dual input operation " + id + ": required inputs=(%s), available inputs=(%s)".format( context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",") ) )
    val i0 = input_arrays(0).toCDFloatArray
    val i1 = input_arrays(1).toCDFloatArray
    val result_array: CDFloatArray = mapCombineOp match {
      case Some( combineOp ) => CDFloatArray.combine( combineOp, i0, i1 )
      case None => i0
    }
    val result_metadata = input_arrays.head.metadata ++ List( "uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile( inputs.elements )  )
    logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, inputs.iPart, (System.nanoTime - t0) / 1.0E9))
    RDDPartition( key, Map( context.operation.rid -> HeapFltArray(result_array, input_arrays(0).origin, result_metadata, None) ), inputs.metadata )
  }
}

abstract class MultiRDDKernel( options: Map[String,String] ) extends Kernel(options)  {

  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val key = inputs.iPart
    val t0 = System.nanoTime
    logger.info("&MAP: Executing Kernel %s[%d]".format(name, inputs.iPart ) )
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val input_arrays: List[ArrayBase[Float]] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[ArrayBase[Float]]())( _ ++ _ )
    assert( input_arrays.size > 1, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format( context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",") ) )
    val cdFloatArrays = input_arrays.map( _.toCDFloatArray ).toArray
    val final_result: CDFloatArray = if( mapCombineNOp.isDefined ) {
      CDFloatArray.combine( mapCombineNOp.get, cdFloatArrays )
    } else if( mapCombineWNOp.isDefined ) {
      val (result_array, countArray) = CDFloatArray.combine( mapCombineWNOp.get, cdFloatArrays )
      result_array / countArray
    } else { throw new Exception("Undefined operation in MultiRDDKernel") }
    logger.info("&MAP: Finished Kernel %s[%d], time = %.4f s".format(name, inputs.iPart, (System.nanoTime - t0) / 1.0E9))
    val result_metadata = input_arrays.head.metadata ++ List( "uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile( inputs.elements )  )
    RDDPartition( key, Map( context.operation.rid -> HeapFltArray(final_result, input_arrays(0).origin, result_metadata, None) ), inputs.metadata )
  }
}

class CDMSRegridKernel extends zmqPythonKernel( "python.cdmsmodule", "regrid", "Regridder", "Regrids the inputs using UVCDAT", Map( "parallelize" -> "True" ) ) {

  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val key = inputs.iPart
    val t0 = System.nanoTime
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance
    val worker: PythonWorker = workerManager.getPythonWorker
    try {
      logger.info("&MAP: Executing Kernel %s[%d]".format(name, inputs.iPart))
      val targetGridSpec: String = context.config("gridSpec", inputs.elements.values.head.gridSpec)
      val input_arrays: List[HeapFltArray] = context.operation.inputs.map(id => inputs.findElements(id)).foldLeft(List[HeapFltArray]())(_ ++ _)
      assert(input_arrays.nonEmpty, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format(context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",")))

      val (acceptable_arrays, regrid_arrays) = input_arrays.partition(_.gridSpec.equals(targetGridSpec))
      if (regrid_arrays.isEmpty) { inputs }
      else {
        for (input_array <- acceptable_arrays) { worker.sendArrayMetadata(inputs.iPart, input_array.uid, input_array) }
        for (input_array <- regrid_arrays)     { worker.sendArrayData(inputs.iPart, input_array.uid, input_array) }
        val acceptable_array_map = Map(acceptable_arrays.map(array => array.uid -> array): _*)

        logger.info("Gateway-%d: Executing operation %s".format(inputs.iPart, context.operation.identifier))
        val context_metadata = indexAxisConf(context.getConfiguration, context.grid.axisIndexMap) + ("gridSpec" -> targetGridSpec )
        val rID = UID()
        worker.sendRequest("python.cdmsModule.regrid-" + rID, regrid_arrays.map(_.uid).toArray, context_metadata )

        val resultItems = for (input_array <- regrid_arrays) yield {
          val tvar = worker.getResult
          val result = HeapFltArray(tvar, input_array.missing, Some(targetGridSpec))
          context.operation.rid + ":" + input_array.uid -> result
        }
        val array_metadata = input_arrays.head.metadata ++ List("uid" -> context.operation.rid, "gridSpec" -> targetGridSpec )
        val array_metadata_crs = context.crsOpt.map( crs => array_metadata + ( "crs" -> crs ) ).getOrElse( array_metadata )
        logger.info("&MAP: Finished Kernel %s[%d], time = %.4f s, metadata = %s".format(name, inputs.iPart, (System.nanoTime - t0) / 1.0E9, array_metadata_crs.mkString(";")))
        RDDPartition(key, Map(resultItems: _*) ++ acceptable_array_map, array_metadata_crs)
      }
    } finally {
      workerManager.releaseWorker( worker )
    }
  }
}

class zmqPythonKernel( _module: String, _operation: String, _title: String, _description: String, options: Map[String,String]  ) extends Kernel(options) {
  override def operation: String = _operation
  override def module = _module
  override def name = _module.split('.').last + "." + _operation
  override def id = _module + "." + _operation
  override val identifier = name
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = _title
  val description = _description

  override def cleanUp = PythonWorkerPortal.getInstance.shutdown

  override def map( inputs: RDDPartition, context: KernelContext  ): RDDPartition = {
    val key = inputs.iPart
    val t0 = System.nanoTime
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance();
    val worker: PythonWorker = workerManager.getPythonWorker();
    try {
      logger.info("&MAP: Executing Kernel %s[%d]".format(name, inputs.iPart))
      val input_arrays: List[HeapFltArray] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[HeapFltArray]())( _ ++ _ )
      assert(input_arrays.size > 0, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format(context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",")))
      val operation_input_arrays = context.operation.inputs.flatMap( input_id => inputs.element( input_id ) )

      for( input_id <- context.operation.inputs ) inputs.element(input_id) match {
        case Some( input_array ) =>
          worker.sendArrayData( inputs.iPart, input_id, input_array )
          logger.info( "Kernel part-%d: Finished Sending data to worker" )
        case None =>
          worker.sendUtility( List( "input", input_id ).mkString(";") )
      }
      logger.info( "Gateway-%d: Executing operation %s".format( inputs.iPart,context.operation.identifier ) )
      val metadata = indexAxisConf( context.getConfiguration, context.grid.axisIndexMap )
      worker.sendRequest(context.operation.identifier, context.operation.inputs.toArray, metadata )
      val resultItems = for( input_array <- operation_input_arrays ) yield {
        val tvar = worker.getResult()
        val result = HeapFltArray( tvar, input_array.missing )
        context.operation.rid + ":" + input_array.uid -> result
      }
      val result_metadata = input_arrays.head.metadata ++ List( "uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile( inputs.elements )  )
      logger.info("&MAP: Finished Kernel %s[%d], time = %.4f s, metadata = %s".format(name, inputs.iPart, (System.nanoTime - t0) / 1.0E9, result_metadata.mkString(";") ) )
      RDDPartition( key, Map(resultItems:_*), result_metadata )
    } finally {
      workerManager.releaseWorker( worker )
    }
  }

  override def customReduceRDD(context: KernelContext)( a0: ( Int, RDDPartition ), a1: ( Int, RDDPartition ) ): ( Int, RDDPartition ) = {
    val ( rdd0, rdd1 ) = ( a0._2, a1._2 )
    val t0 = System.nanoTime
    logger.info("&MERGE: start (%d <-> %d)".format( rdd0.iPart, rdd1.iPart  ) )
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance
    val worker: PythonWorker = workerManager.getPythonWorker
    val ascending = rdd0.iPart < rdd1.iPart
    val op_metadata = indexAxisConf( context.getConfiguration, context.grid.axisIndexMap )
    rdd0.elements.map {
      case (key, element0) =>  rdd1.elements.get(key).map( element1 => key -> {
        val (array0, array1) = if (ascending) (element0, element1) else (element1, element0)
        val uids = Array( s"${rdd0.iPart}.${array0.uid}", s"${rdd1.iPart}.${array1.uid}" )
        worker.sendArrayData( rdd0.iPart, uids(0), array0 )
        worker.sendArrayData( rdd1.iPart, uids(1), array1 )
        worker.sendRequest( context.operation.identifier, uids, Map( "action" -> "reduce", "axes" -> context.getAxes.getAxes.mkString(",") ) )
      })
    }
    val resultItems = rdd0.elements.map {
      case (key, element0) =>
        val tvar = worker.getResult
        val result = HeapFltArray( tvar, element0.missing )
        context.operation.rid + ":" + element0.uid -> result
    }
    logger.info("&MERGE: finish (%d <-> %d), time = %.4f s".format( rdd0.iPart, rdd1.iPart, (System.nanoTime - t0) / 1.0E9 ) )
    a0._1 -> RDDPartition( rdd0.iPart, resultItems, rdd0.mergeMetadata("merge", rdd1) )
  }

  def indexAxisConf( metadata: Map[String,String], axisIndexMap: Map[String,Int] ): Map[String,String] = {
    try {
      metadata.get("axes") match {
        case None => metadata
        case Some(axis_spec) =>
          val axisIndices = axis_spec.map( _.toString).map( axis => axisIndexMap(axis) )
          metadata + ( "axes" -> axisIndices.mkString(""))
      }
    } catch { case e: Exception => throw new Exception( "Error converting axis spec %s to indices using axisIndexMap {%s}: %s".format( metadata.get("axes"), axisIndexMap.mkString(","), e.toString ) )  }
  }
}

class TransientFragment( val dataFrag: DataFragment, val request: RequestContext, val varMetadata: Map[String,nc2.Attribute] ) extends OperationDataInput( dataFrag.spec, varMetadata ) {
  def toXml(id: String): xml.Elem = {
    val units = varMetadata.get("units") match { case Some(attr) => attr.getStringValue; case None => "" }
    val long_name = varMetadata.getOrElse("long_name",varMetadata.getOrElse("fullname",varMetadata.getOrElse("varname", new Attribute("varname","UNDEF")))).getStringValue
    val description = varMetadata.get("description") match { case Some(attr) => attr.getStringValue; case None => "" }
    val axes = varMetadata.get("axes") match { case Some(attr) => attr.getStringValue; case None => "" }
    <result id={id} missing_value={dataFrag.data.getInvalid.toString} shape={dataFrag.data.getShape.mkString("(",",",")")} units={units} long_name={long_name} description={description} axes={axes}> { dataFrag.data.mkBoundedDataString( ", ", 1100 ) } </result>
  }
  def domainDataFragment( partIndex: Int,  optSection: Option[ma2.Section]  ): Option[DataFragment] = Some(dataFrag)
  def data(partIndex: Int ): CDFloatArray = dataFrag.data
  def delete() = {;}
}

class SerializeTest {
  val input_array: CDFloatArray = CDFloatArray.const( Array(4), 2.5f )
  val ucar_array = CDFloatArray.toUcarArray( input_array )
  val byte_data = ucar_array.getDataAsByteBuffer().array()
  println( "Byte data: %x %x %x %x".format( byte_data(0),byte_data(1), byte_data(2), byte_data(3) ))
  val tvar = new TransVar( " | |0|4| ", byte_data )
  val result = HeapFltArray( tvar, None )
  println( "Float data: %f %f %f %f".format( result.data(0), result.data(1), result.data(2), result.data(3) ))
}

class zmqSerializeTest {
  import nasa.nccs.cdas.workers.test.floatClient
  val input_array: CDFloatArray = CDFloatArray.const( Array(4), 2.5f )
  val ucar_array = CDFloatArray.toUcarArray( input_array )
  val byte_data = ucar_array.getDataAsByteBuffer().array()
  println( "Byte data: %d %d %d %d".format( byte_data(0),byte_data(1), byte_data(2), byte_data(3) ))
  floatClient.run( byte_data )
}
