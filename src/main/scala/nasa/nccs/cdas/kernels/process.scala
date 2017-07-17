package nasa.nccs.cdas.kernels

import java.io._

import scala.xml
import java.nio.{ByteBuffer, ByteOrder, FloatBuffer}

import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data.{HeapFltArray, _}
import nasa.nccs.cdapi.tensors.CDFloatArray.{ReduceNOpFlt, ReduceOpFlt, ReduceWNOpFlt}
import nasa.nccs.cdapi.tensors.{CDArray, CDCoordMap, CDFloatArray, CDTimeCoordMap}
import nasa.nccs.cdas.engine.spark.RecordKey
import nasa.nccs.cdas.modules.CDSpark.BinKeyUtils
import nasa.nccs.cdas.workers.TransVar
import nasa.nccs.cdas.workers.python.{PythonWorker, PythonWorkerPortal}
import nasa.nccs.cdas.utilities.{appParameters, runtime}
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.{Loggable, ProfilingTool}
import nasa.nccs.wps.{WPSProcess, WPSProcessOutput}
import org.apache.spark.rdd.RDD
import ucar.nc2.Attribute
import ucar.{ma2, nc2}

import scala.collection.JavaConversions._
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable
import scala.collection.mutable.SortedSet

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

class KernelContext( val operation: OperationContext, val grids: Map[String,Option[GridContext]], val sectionMap: Map[String,Option[CDSection]], val domains: Map[String,DomainContainer],  _configuration: Map[String,String], val profiler: ProfilingTool ) extends Loggable with Serializable with ScopeContext {
  val crsOpt = getCRS
  val trsOpt = getTRS
  val timings: SortedSet[(Float,String)] = SortedSet.empty
  val configuration = crsOpt.map( crs => _configuration + ("crs" -> crs ) ) getOrElse( _configuration )
  lazy val grid: GridContext = getTargetGridContext
  def findGrid( varUid: String ): Option[GridContext] = grids.find( item => item._1.split('-')(0).equals(varUid) ).flatMap( _._2 )
  def getConfiguration = configuration ++ operation.getConfiguration
  def getAxes: AxisIndices = grid.getAxisIndices( config("axes", "") )
  def getContextStr: String = getConfiguration map { case ( key, value ) => key + ":" + value } mkString ";"
  def getDomainMetadata(domId: String): Map[String,String] = domains.get(domId) match { case Some(dc) => dc.metadata; case None => Map.empty }
  def findAnyGrid: GridContext = (grids.find { case (k, v) => v.isDefined }).getOrElse(("", None))._2.getOrElse(throw new Exception("Undefined grid in KernelContext for op " + operation.identifier))
  private def getCRS: Option[String] =
    operation.getDomain flatMap ( domId => domains.get( domId ).flatMap ( dc => dc.metadata.get("crs") ) )
  private def getTRS: Option[String] = operation.getDomain flatMap ( domId => domains.get( domId ).flatMap ( dc => dc.metadata.get("trs") ) )
  def conf( params: Map[String,String] ): KernelContext = new KernelContext( operation, grids, sectionMap, domains, configuration ++ params, profiler )
  def commutativeReduction: Boolean = if( getAxes.includes(0) ) { true } else { false }
  def doesTimeReduction: Boolean = getAxes.includes(0)
  def addTimestamp( label: String, log: Boolean = false ): Unit = {
    profiler.timestamp(label)
    if( log ) { logger.info(label) }
  }

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

object Kernel extends Loggable {
  val customKernels = List[Kernel]( new CDMSRegridKernel() )
  type RDDKeyValPair = ( RecordKey, RDDRecord )

  def getResultFile( resultId: String, deleteExisting: Boolean = false ): File = {
    val resultsDirPath = appParameters("wps.results.dir", "~/.wps/results").replace( "~",  System.getProperty("user.home") ).replaceAll("[()]","-").replace("=","~")
    val resultsDir = new File(resultsDirPath); resultsDir.mkdirs()
    val resultFile = new File( resultsDirPath + s"/$resultId.nc" )
    if( deleteExisting && resultFile.exists ) resultFile.delete
    resultFile
  }

  def mergeRDD(context: KernelContext)(a0: RDDKeyValPair, a1: RDDKeyValPair ): RDDKeyValPair = {
    val ( rdd0, rdd1 ) = ( a0._2, a1._2 )
    val ( k0, k1 ) = ( a0._1, a1._1 )
    val t0 = System.nanoTime
    logger.info("&MERGE: start (%s <-> %s), sample rdd0 = %s, rdd1 = %s".format( k0.toString, k1.toString, rdd0.head._2.getSampleDataStr(10,0), rdd1.head._2.getSampleDataStr(10,0)  ) )
    val new_key = k0 + k1
    val new_elements = rdd0.elements.flatMap {
      case (elkey, element0) =>  rdd1.elements.get(elkey).map( element1 => elkey -> { if( k0.start <= k1.start ) { element0.append(element1) } else { element1.append(element0) } } )
    }
    val dt = (System.nanoTime - t0) / 1.0E9
    logger.info("&MERGE: complete in time = %.4f s, result sample = %s".format( dt, new_elements.head._2.getSampleDataStr(10,0) ) )
    context.addTimestamp("&MERGE: complete, time = %.4f s, key = ".format( dt, new_key.toString ) )
    new_key -> RDDRecord( new_elements, rdd0.mergeMetadata("merge", rdd1) )
  }

  def orderedMergeRDD(context: KernelContext)(a0: RDDKeyValPair, a1: RDDKeyValPair ): RDDKeyValPair = {
    val ( rdd0, rdd1 ) = ( a0._2, a1._2 )
    val ( k0, k1 ) = ( a0._1, a1._1 )
    val t0 = System.nanoTime
    val new_key = k0 + k1
    val new_elements = rdd0.elements.map { case (key, array) => (key+"%"+k0.elemStart, array) } ++ rdd1.elements.map { case (key, array) => (key+"%"+k1.elemStart, array) }
    val dt = (System.nanoTime - t0) / 1.0E9
    context.addTimestamp("&MERGE: complete, time = %.4f s, key = ".format( dt, new_key.toString ) )
    new_key -> RDDRecord( new_elements, rdd0.mergeMetadata("merge", rdd1) )
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
  def getWeights( inputId: String, context: KernelContext, weighting_type_opt: Option[String]=None, broadcast: Boolean = true ): CDFloatArray =  {
    val weighting_type = weighting_type_opt.getOrElse( context.config("weights", if (context.config("axes", "").contains('y')) "cosine" else "") )
    val t0 = System.nanoTime
    val weights = context.sectionMap.get( inputId ).flatten match {
      case Some(section) =>
        weighting_type match {
          case "cosine" =>
            context.grid.getSpatialAxisData('y', section) match {
              case Some(axis_data) => computeWeights( weighting_type, Map('y' -> axis_data), section.getShape, Float.MaxValue, broadcast )
              case None => logger.warn("Can't access AxisData for variable %s => Using constant weighting.".format(inputId)); CDFloatArray.const(section.getShape, 1f)
            }
          case x =>
            if (!x.isEmpty) { logger.warn("Can't recognize weighting method: %s => Using constant weighting.".format(x)) }
            CDFloatArray.const(section.getShape, 1f)
        }
      case None => CDFloatArray.empty
    }
    logger.info( "Computed weights in time %.4f s".format(  (System.nanoTime - t0) / 1.0E9 ) )
    weights
  }

  def computeWeights( weighting_type: String, axisDataMap: Map[ Char, ( Int, ma2.Array ) ], shape: Array[Int], invalid: Float, broadcast: Boolean ) : CDFloatArray  = {
    weighting_type match {
      case "cosine" =>
        axisDataMap.get('y') match {
          case Some( ( axisIndex, yAxisData ) ) =>
            val axis_length = yAxisData.getSize
            val axis_data = CDFloatArray.factory( yAxisData, Float.MaxValue )
            assert( axis_length == shape(axisIndex), "Y Axis data mismatch, %d vs %d".format(axis_length,shape(axisIndex) ) )
            val cosineWeights: CDFloatArray = axis_data.map( x => Math.cos( Math.toRadians(x) ).toFloat )
            val base_shape: Array[Int] = Array( shape.indices.map(i => if(i==axisIndex) shape(axisIndex) else 1 ): _* )
            val weightsArray: CDArray[Float] =  CDArray( base_shape, cosineWeights.getStorage, invalid )
            if(broadcast) { weightsArray.broadcast( shape ) }
            weightsArray
          case None => throw new NoSuchElementException( "Missing axis data in weights computation, type: %s".format( weighting_type ))
        }
      case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
    }
  }
}

class KIType { val Op = 0; val MData = 1 }

object PartSortUtils {
  implicit object PartSortOrdering extends Ordering[String] {
    def compare( k1: String, k2: String ) = k1.split('%')(1).toInt - k2.split('%')(1).toInt
  }
}

abstract class Kernel( val options: Map[String,String] = Map.empty ) extends Loggable with Serializable with WPSProcess {
  import Kernel._
  val identifiers = this.getClass.getName.split('$').flatMap(_.split('.'))
  def operation: String = identifiers.last.toLowerCase
  def module: String = identifiers.dropRight(1).mkString(".")
  def id = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")
  val extInputs: Boolean = options.getOrElse("handlesInput","false").toBoolean
  val parallelizable: Boolean = options.getOrElse( "parallelize", (!extInputs).toString ).toBoolean
  val identifier = name
  def matchesSpecs( specs: Array[String] ): Boolean = { (specs.size >= 2) && specs(0).equals(module) && specs(1).equals(operation) }
  val nOutputsPerInput: Int = options.getOrElse("nOutputsPerInput","1").toInt
  val weightsOpt: Option[String] = options.get("weights")

  val mapCombineOp: Option[ReduceOpFlt] = options.get("mapOp").fold (options.get("mapreduceOp")) (Some(_)) map CDFloatArray.getOp
  val mapCombineNOp: Option[ReduceNOpFlt] = None
  val mapCombineWNOp: Option[ReduceWNOpFlt] = None
  val reduceCombineOp: Option[ReduceOpFlt] = options.get("reduceOp").fold (options.get("mapreduceOp")) (Some(_)) map CDFloatArray.getOp
  val initValue: Float = 0f
  def cleanUp() = {}


  def addWeights( context: KernelContext ): Boolean = {
    weightsOpt match {
      case Some( weights ) =>
        val axes = context.operation.getConfiguration("axes")
        if( weights == "cosine" ) { axes.indexOf( "y" ) > -1 }
        else throw new Exception( "Unrecognized weights type: " + weights )
      case None => false

    }
  }

  def getReduceOp(context: KernelContext): (RDDKeyValPair,RDDKeyValPair)=>RDDKeyValPair =
    if (context.doesTimeReduction) {
      reduceCombineOp match {
        case Some(redOp) => redOp match {
          case CDFloatArray.customOp => customReduceRDD(context)
          case op => reduceRDDOp(context)
        }
        case None => reduceRDDOp(context)
      }
    } else {
      orderedMergeRDD(context)
    }

  def getOpName(context: KernelContext): String = "%s(%s)".format(name, context.operation.inputs.mkString(","))
  def map(partIndex: Int, inputs: List[Option[DataFragment]], context: KernelContext): Option[DataFragment] = inputs.head
  def map(context: KernelContext )( rdd: RDDRecord ): RDDRecord = { rdd }
  def aggregate(context: KernelContext )( rdd0: RDDRecord, rdd1: RDDRecord ): RDDRecord = { rdd0 }

  def keyMapper( partIndex: Int, agg_inputs: Iterator[(RecordKey,RDDRecord)] ): Iterator[(RecordKey,RDDRecord)] = {
    val result = agg_inputs.flatMap { case ( agg_key, agg_record ) => ( 0 until agg_record.getShape(0) ) map ( tindex => ( agg_key.singleElementKey(tindex), agg_record.slice(tindex,1) ) ) }
    logger.info( s"KeyMapper for part ${partIndex}, size = " + result.length )
    result
  }

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


  def combineRDDLegacy(context: KernelContext)(rdd0: RDDRecord, rdd1: RDDRecord ): RDDRecord = {
    val t0 = System.nanoTime
    val axes = context.getAxes
    val key_group_prefixes: Set[String] = rdd0.elements.keys.map( key => key.split("~").head ).toSet
    val key_groups: Set[(String,IndexedSeq[String])] = key_group_prefixes map ( key_prefix =>  key_prefix -> rdd0.elements.keys.filter( _.split("~").head.equals( key_prefix ) ).toIndexedSeq )
    val new_elements: IndexedSeq[(String,HeapFltArray)] = key_groups.toIndexedSeq.flatMap { case ( group_key, key_group ) =>
      val elements0: IndexedSeq[(String,HeapFltArray)] = key_group flatMap ( key => rdd0.elements.filter { case (k,v) => if(key.contains("_WEIGHTS_")) k.equals(key) else k.startsWith(key) } )
      val elements1: IndexedSeq[(String,HeapFltArray)] = key_group flatMap ( key => rdd1.elements.filter { case (k,v) => if(key.contains("_WEIGHTS_")) k.equals(key) else k.startsWith(key) } )
      if( elements0.size != elements1.size ) {
        throw new Exception( s"Mismatched rdds in reduction for kernel ${context.operation.identifier}: ${elements0.size} != ${elements1.size}" )
      }
      if( elements0.size != nOutputsPerInput ) {
        throw new Exception( s"Wrong number of elements in reduction rdds for kernel ${context.operation.identifier}: ${elements0.size} != ${nOutputsPerInput}, element keys = [${elements0.map(_._1).mkString(",")}]" ) }
      if( elements0.size != elements1.size ) {
        throw new Exception( s"Mismatched rdds in reduction for kernel ${context.operation.identifier}: ${elements0.size} != ${elements1.size}" )
      }
      if( elements0.size == 1 ) {
        reduceCombineOp match {
          case Some(combineOp) =>
            if (axes.includes(0)) IndexedSeq(group_key -> elements0(0)._2.combine(combineOp, elements1(0)._2))
            else IndexedSeq(group_key -> elements0(0)._2.append(elements1(0)._2))
          case None => IndexedSeq(group_key -> elements0(0)._2.append(elements1(0)._2))
        }
      } else {
        reduceCombineOp match {
          case Some(combineOp) =>
            if (axes.includes(0)) combineElements( group_key, Map(elements0:_*), Map(elements1:_*) )
            else appendElements( group_key, Map(elements0:_*), Map(elements1:_*) )
          case None => appendElements( group_key, Map(elements0:_*), Map(elements1:_*) )
        }
      }
    }
//    logger.debug("&COMBINE: %s, time = %.4f s".format( context.operation.name, (System.nanoTime - t0) / 1.0E9 ) )
    context.addTimestamp( "combineRDD complete" )
    RDDRecord( TreeMap(new_elements:_*), rdd0.mergeMetadata(context.operation.name, rdd1) )
  }

  def combineRDD(context: KernelContext)(rdd0: RDDRecord, rdd1: RDDRecord ): RDDRecord = {
    val t0 = System.nanoTime
    val axes = context.getAxes
    val elements0: IndexedSeq[(String,HeapFltArray)] = rdd0.elements.toIndexedSeq
    val keys = rdd0.elements.keys
    if( keys.size != nOutputsPerInput ) {
      throw new Exception( s"Wrong number of elements in reduction rdds for kernel ${context.operation.identifier}: ${keys.size} != ${nOutputsPerInput}, element keys = [${keys.mkString(",")}]" ) }
    val new_elements: IndexedSeq[(String,HeapFltArray)] = elements0 flatMap { case ( key0, array0 ) => rdd1.elements.get(key0) match {
        case Some(array1) => reduceCombineOp match {
            case Some(combineOp) =>
              if (axes.includes(0)) Some( key0 -> array0.combine( combineOp, array1 ) )
              else Some(key0 -> array0.append(array1))
            case None => Some(key0 -> array0.append(array1))
          }
        case None => None
      }
    }
    //    logger.debug("&COMBINE: %s, time = %.4f s".format( context.operation.name, (System.nanoTime - t0) / 1.0E9 ) )
    context.addTimestamp( "combineRDD complete" )
    RDDRecord( TreeMap(new_elements:_*), rdd0.mergeMetadata(context.operation.name, rdd1) )
  }

  def combineElements( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] = {
    options.get("reduceOp") match {
      case Some( reduceOp ) =>
        if( reduceOp.toLowerCase == "sumw" ) {
          weightedSumReduction( key, elements0, elements1 )
        } else if( reduceOp.toLowerCase == "avew" ) {
          weightedAveReduction( key, elements0, elements1 )
        } else {
          throw new Exception( s"Unimplemented multi-input reduce op for kernel ${identifier}: " + reduceOp )
        }
      case None =>
        logger.warn( s"No reduce op defined for kernel ${identifier}, appending elements" )
        appendElements( key, elements0, elements1 )
    }
  }

  def missing_element( key: String ) = throw new Exception( s"Missing element in weightedSumReduction for Kernel ${identifier}, key: " + key )

  def getFloatBuffer( size: Int ): FloatBuffer = {
    val vbb: ByteBuffer = ByteBuffer.allocateDirect( size * 4 )
    vbb.order( ByteOrder.nativeOrder() );    // use the device hardware's native byte order
    vbb.asFloatBuffer();
  }

  def weightedSumReduction( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] = {
    val key_lists = elements0.keys.partition( _.endsWith("_WEIGHTS_") )
    val weights_key = key_lists._1.headOption.getOrElse( throw new Exception( s"Can't find weignts key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
    val values_key  = key_lists._2.headOption.getOrElse( throw new Exception( s"Can't find values key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
    val weights0 = elements0.getOrElse( weights_key, missing_element(key) )
    val weights1 = elements1.getOrElse( weights_key, missing_element(key) )
    val values0 = elements0.getOrElse( values_key, missing_element(key) )
    val values1 = elements1.getOrElse( values_key, missing_element(key) )
    val t0 = System.nanoTime()
    val resultWeights = FloatBuffer.allocate( values0.data.length )
    val resultValues = FloatBuffer.allocate(  weights0.data.length )
    values0.missing match {
      case Some( undef ) =>
        for( index <- values0.data.indices; v0 = values0.data(index); v1 = values1.data(index) ) {
          if( v0 == undef || v0.isNaN ) {
            if( v1 == undef || v1.isNaN ) {
              resultValues.put( index, undef )
            } else {
              resultValues.put( index, v1 )
              resultWeights.put( index, weights1.data(index) )
            }
          } else if( v1 == undef || v1.isNaN ) {
            resultValues.put( index, v0 )
            resultWeights.put( index, weights0.data(index) )
          } else {
            val w0 = weights0.data(index)
            val w1 = weights1.data(index)
            resultValues.put( index, v0 + v1 )
            resultWeights.put( index,  w0 + w1 )
          }
        }
      case None =>
        for( index <- values0.data.indices ) {
          resultValues.put( values0.data(index) + values1.data(index) )
          resultWeights.put( weights0.data(index) + weights1.data(index) )
        }
    }
    val valuesArray =  HeapFltArray( CDFloatArray( values0.shape,  resultValues.array,  values0.missing.getOrElse(Float.MaxValue) ),  values0.origin,  values0.metadata,  values0.weights  )
    val weightsArray = HeapFltArray( CDFloatArray( weights0.shape, resultWeights.array, weights0.missing.getOrElse(Float.MaxValue) ), weights0.origin, weights0.metadata, weights0.weights )
    logger.info("Completed weightedSumReduction '%s' in %.4f sec, shape = %s".format(identifier, ( System.nanoTime() - t0 ) / 1.0E9, values0.shape.mkString(",") ) )
    IndexedSeq( values_key -> valuesArray, weights_key -> weightsArray )
  }

  def weightedAveReduction( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] = {
    val key_lists = elements0.keys.partition( _.endsWith("_WEIGHTS_") )
    val weights_key = key_lists._1.headOption.getOrElse( throw new Exception( s"Can't find weignts key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
    val values_key  = key_lists._2.headOption.getOrElse( throw new Exception( s"Can't find values key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
    val weights0 = elements0.getOrElse( weights_key, missing_element(key) )
    val weights1 = elements1.getOrElse( weights_key, missing_element(key) )
    val values0 = elements0.getOrElse( values_key, missing_element(key) )
    val values1 = elements1.getOrElse( values_key, missing_element(key) )
    val t0 = System.nanoTime()
    val weightsSum = FloatBuffer.allocate( values0.data.length )
    val weightedValues0 = FloatBuffer.allocate(  values0.data.length )
    val weightedValues1 = FloatBuffer.allocate(  values0.data.length )
    values0.missing match {
      case Some( undef ) =>
        for( index <- values0.data.indices; v0 = values0.data(index); v1 = values1.data(index) ) {
          if( v0 == undef || v0.isNaN ) {
            if( v1 == undef || v1.isNaN ) {
              weightedValues0.put( index, undef )
              weightedValues1.put( index, undef )
            } else {
              weightedValues0.put( index, undef )
              weightedValues1.put( index, v1*weights1.data(index) )
              weightsSum.put( index, weights1.data(index) )
            }
          } else if( v1 == undef || v1.isNaN ) {
            weightedValues0.put( index, v0*weights0.data(index) )
            weightedValues1.put( index, undef )
            weightsSum.put( index, weights0.data(index) )
          } else {
            weightedValues0.put( index, values0.data(index) * weights0.data(index) )
            weightedValues1.put( index, values1.data(index) * weights1.data(index) )
            weightsSum.put( index, weights0.data(index) + weights1.data(index) )
          }
        }
        for( index <- values0.data.indices; wv0 = weightedValues0.get(index); wv1 = weightedValues1.get(index); ws = weightsSum.get(index) ) {
          if( wv0 == undef ) {
            if (wv1 == undef) { weightedValues0.put(index, undef) } else { weightedValues0.put(index, wv1/ws) }
          } else if (wv1 == undef) { weightedValues0.put(index, wv0 / ws) }
          else {
            weightedValues0.put( index,  (wv0 + wv1) / ws )
          }
        }
      case None =>
        for( index <- values0.data.indices ) {
          weightedValues0.put( index, values0.data(index) * weights0.data(index) )
          weightedValues1.put( index, values1.data(index) * weights1.data(index) )
          weightsSum.put( index, weights0.data(index) + weights1.data(index) )
        }
        for( index <- values0.data.indices ) {
          weightedValues0.put( index, (weightedValues0.get(index) + weightedValues1.get(index)) / weightsSum.get(index) )
        }
    }
    val valuesArray =  HeapFltArray( CDFloatArray( values0.shape,  weightedValues0.array,  values0.missing.getOrElse(Float.MaxValue) ),  values0.origin,  values0.metadata,  values0.weights  )
    val weightsArray = HeapFltArray( CDFloatArray( weights0.shape, weightsSum.array, weights0.missing.getOrElse(Float.MaxValue) ), weights0.origin, weights0.metadata, weights0.weights )
    logger.info("Completed weightedAveReduction '%s' in %.4f sec, shape = %s".format(identifier, ( System.nanoTime() - t0 ) / 1.0E9, values0.shape.mkString(",") ) )
    IndexedSeq( values_key -> valuesArray, weights_key -> weightsArray )
  }

  def appendElements( key: String,  elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] = {
    elements0 flatMap { case (key,fltArray) => elements1.get(key) map ( fltArray1 => key -> fltArray.append(fltArray1) ) } toIndexedSeq
  }

  def customReduceRDD(context: KernelContext)(a0: RDDKeyValPair, a1: RDDKeyValPair ): RDDKeyValPair = {
    logger.warn( s"No reducer defined for parallel op '$name', executing simple merge." )
    mergeRDD(context)( a0, a1 )
  }

  def postOp(result: DataFragment, context: KernelContext): DataFragment = result

  def orderElements( op_result: RDDRecord, context: KernelContext ): RDDRecord = if ( context.doesTimeReduction ) { op_result } else {
    val sorted_keys = op_result.elements.keys.toIndexedSeq.sortBy( key => key.split('%')(1).toInt )
    val resultMap = mutable.HashMap.empty[String,HeapFltArray]
    sorted_keys.foreach( key => op_result.elements.get(key) match {
      case Some( array ) =>
        val base_key = key.split('%').head
        resultMap.get( base_key ) match {
          case Some( existing_array ) => resultMap.put( base_key, existing_array.append(array) )
          case None => resultMap.put( base_key, array )
        }
      case None => Unit
    })
    new RDDRecord( TreeMap( resultMap.toIndexedSeq:_* ), op_result.metadata )
  }

  def postRDDOp( pre_result: RDDRecord, context: KernelContext ): RDDRecord = {
    options.get("postOp") match {
      case Some( postOp ) =>
        if( postOp == "normw") {
          val key_lists = pre_result.elements.keys.partition( _.endsWith("_WEIGHTS_") )
          val weights_key = key_lists._1.headOption.getOrElse( throw new Exception( s"Can't find weignts key in postRDDOp for Kernel ${identifier}, keys: " + pre_result.elements.keys.mkString(",") ) )
          val values_key  = key_lists._2.headOption.getOrElse( throw new Exception( s"Can't find values key in postRDDOp for Kernel ${identifier}, keys: " + pre_result.elements.keys.mkString(",") ) )
          val weights = pre_result.elements.getOrElse( weights_key, missing_element(weights_key) )
          val values = pre_result.elements.getOrElse( values_key, missing_element(values_key) )
          val averageValues = FloatBuffer.allocate(  values.data.length )
          values.missing match {
            case Some( undef ) =>
              for( index <- values.data.indices; value = values.data(index) ) {
                if( value == undef || value.isNaN  ) { undef }
                else {
                  val wval =  weights.data(index)
                  averageValues.put( value / wval )
                }
              }
            case None =>
              for( index <- values.data.indices ) { averageValues.put( values.data(index) / weights.data(index) ) }
          }
          val valuesArray =  HeapFltArray( CDFloatArray( values.shape,  averageValues.array,  values.missing.getOrElse(Float.MaxValue) ),  values.origin,  values.metadata,  values.weights  )
          context.addTimestamp( "postRDDOp complete" )
          new RDDRecord( TreeMap( values_key -> valuesArray ), pre_result.metadata )
        } else if( (postOp == "sqrt") || (postOp == "rms") ) {
          val new_elements = pre_result.elements map { case (values_key, values) =>
            val averageValues = FloatBuffer.allocate(values.data.length)
            values.missing match {
              case Some(undef) =>
                if( postOp == "sqrt" ) {
                  for (index <- values.data.indices; value = values.data(index)) {
                    if (value == undef || value.isNaN  ) { undef }
                    else { averageValues.put(Math.sqrt(value).toFloat) }
                  }
                } else if( postOp == "rms" ) {
                  val axes = context.config("axes", "").toUpperCase // values.metadata.getOrElse("axes","")
                  val roi: ma2.Section = CDSection.deserialize( values.metadata.getOrElse("roi","") )
                  val reduce_ranges = axes.flatMap( axis => CDSection.getRange( roi, axis.toString ) )
                  val norm_factor = reduce_ranges.map( _.length() ).fold(1)(_ * _) - 1
                  if( norm_factor == 0 ) { throw new Exception( "Missing or unrecognized 'axes' parameter in rms reduce op")}
                  for (index <- values.data.indices; value = values.data(index)) {
                    if (value == undef || value.isNaN  ) { undef }
                    else { averageValues.put(Math.sqrt(value/norm_factor).toFloat  ) }
                  }
                }
              case None =>
                if( postOp == "sqrt" ) {
                  for (index <- values.data.indices) {
                    averageValues.put(Math.sqrt(values.data(index)).toFloat)
                  }
                } else if( postOp == "rms" ) {
                  val norm_factor = values.metadata.getOrElse("N", "1").toInt - 1
                  if( norm_factor == 1 ) { logger.error( "Missing norm factor in rms") }
                  for (index <- values.data.indices) {
                    averageValues.put( Math.sqrt(values.data(index)/norm_factor).toFloat  )
                  }
                }
            }
            val newValuesArray = HeapFltArray(CDFloatArray(values.shape, averageValues.array, values.missing.getOrElse(Float.MaxValue)), values.origin, values.metadata, values.weights)
            ( values_key -> newValuesArray )
          }
          context.addTimestamp( "postRDDOp complete" )
          new RDDRecord( new_elements, pre_result.metadata )
        }
        else { throw new Exception( "Unrecognized postOp configuration: " + postOp ) }
      case None => pre_result
    }
  }

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

  def reduceRDDOp(context: KernelContext)(a0: RDDKeyValPair, a1: RDDKeyValPair ): RDDKeyValPair = (a0._1 + a1._1) -> combineRDD(context)( a0._2, a1._2 )

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

  def fltArray(a0: RDDRecord, elem: String): ( CDFloatArray, Float ) = a0.element(elem) match {
    case Some(data) => ( data.toCDFloatArray, data.getMissing() );
    case None => throw new Exception("Error missing array element: " + elem)
  }
  def toFastMaskedArray(a0: RDDRecord, elem: String): FastMaskedArray = a0.element(elem) match {
    case Some(data) => data.toFastMaskedArray
    case None => throw new Exception("Error missing array element: " + elem)
  }

  def optFltArray(a0: RDDRecord, elem: String): Option[CDFloatArray] = a0.element(elem).map(_.toCDFloatArray)

  def wtArray(a0: RDDRecord, elem: String): Option[CDFloatArray] = a0.element(elem).flatMap( _.toCDWeightsArray )
  def wtFastMaskedArray(a0: RDDRecord, elem: String): Option[FastMaskedArray] = a0.element(elem).flatMap( _.toMa2WeightsArray )

  def originArray(a0: RDDRecord, elem: String): Array[Int]  = a0.element(elem) match {
    case Some(data) => data.origin;
    case None => throw new Exception("Error missing array element: " + elem)
  }

  def arrayMdata(a0: RDDRecord, elem: String): Map[String, String] = a0.element(elem) match {
    case Some(data) => data.metadata;
    case None => Map.empty
  }

  def weightedValueSumRDDCombiner( context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord = {
    val axes = context.getAxes
    if (axes.includes(0)) {
      val t0 = System.nanoTime
      val elems = a0.elements flatMap { case (key, data0) =>
        a1.elements.get( key ) match {
          case Some( data1 ) =>
            val vTot: FastMaskedArray = data0.toFastMaskedArray + data1.toFastMaskedArray
            val t1 = System.nanoTime
            val wTotOpt: Option[Array[Float]] = data0.toMa2WeightsArray flatMap { wtsArray0 => data1.toMa2WeightsArray map { wtsArray1 => (wtsArray0 + wtsArray1).toFloatArray } }
            val t2 = System.nanoTime
            val array_mdata = MetadataOps.mergeMetadata (context.operation.name) (data0.metadata, data1.metadata )
            Some( key -> HeapFltArray (vTot.toCDFloatArray, data0.origin, array_mdata, wTotOpt) )
          case None => logger.warn("Missing elemint in Record combine: " + key); None
        }
      }
      val part_mdata = MetadataOps.mergeMetadata( context.operation.name )( a0.metadata, a1.metadata )
      val t3 = System.nanoTime
      context.addTimestamp( "weightedValueSumCombiner complete" )
      new RDDRecord( elems, part_mdata )
    }
    else {
      a0 ++ a1
    }
  }

//  def weightedValueSumRDDPostOpLegacy(result: RDDRecord, context: KernelContext): RDDRecord = {
//    val rid = context.operation.rid
//    wtFastMaskedArray( result, rid ) match {
//      case Some(w0) =>
//        val v0 = toFastMaskedArray(result, rid)
//        val vOrigin: Array[Int] = originArray(result, rid)
//        logger.info("weightedValueSumPostOp, values shape = %s, weights shape = %s, result spec = %s, values sample = [ %s ], weights sample = [ %s ]".format(v0.array.getShape.mkString(","), w0.array.getShape.mkString(","), result.metadata.toString, v0.toCDFloatArray.mkBoundedDataString(", ",16), w0.toCDFloatArray.mkBoundedDataString(", ",16)))
//        context.addTimestamp( "weightedValueSumPostOp complete" )
//        new RDDRecord( Map(rid -> HeapFltArray( (v0 / w0).toCDFloatArray, vOrigin, arrayMdata(result, "value"), Some( w0.toCDFloatArray.getArrayData() ) ) ), result.metadata )
//      case None =>
//        logger.info("weightedValueSumPostOp: NO WEIGHTS!, Elems:")
//        result.elements.foreach { case (key, heapFltArray) => logger.info(" ** key: %s, values sample = [ %s ]".format( key, heapFltArray.toCDFloatArray.mkBoundedDataString(", ",16)) ) }
//        result
//    }
//  }

  def weightedValueSumRDDPostOp(result: RDDRecord, context: KernelContext): RDDRecord = {
    val new_elements = result.elements map { case (key, fltArray ) =>
      fltArray.toMa2WeightsArray match {
        case Some( wtsArray ) => (key, HeapFltArray( ( fltArray.toFastMaskedArray / wtsArray ).toCDFloatArray, fltArray.origin, fltArray.metadata, None ) )
        case None => (key, fltArray )
      }
    }
//    logger.info( "weightedValueSumPostOp:, Elems:" )
//    new_elements.foreach { case (key, heapFltArray) => logger.info(" ** key: %s, values sample = [ %s ]".format( key, heapFltArray.toCDFloatArray.mkBoundedDataString(", ",16)) ) }
    new RDDRecord( new_elements, result.metadata )
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

abstract class SingularRDDKernel( options: Map[String,String] = Map.empty ) extends Kernel(options)  {
  override def map ( context: KernelContext ) ( inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val async = context.config("async", "false").toBoolean
    val inputId = context.operation.inputs.headOption.getOrElse("NULL")
    val shape = inputs.elements.head._2.shape
    logger.debug(" ##### KERNEL [%s]: Map Op: combine, input shape = %s".format( name, shape.mkString(",") ) )
    runtime.printMemoryUsage
    val elem = inputs.findElements(inputId).headOption match {
      case Some( input_array ) =>
        mapCombineOp match {
          case Some(combineOp) =>
            val cdinput = input_array.toFastMaskedArray
            val result = cdinput.reduce(combineOp, axes.args, initValue).toCDFloatArray
//            logger.info( "Input data sample = [ %s ]".format(cdinput.toCDFloatArray.getArrayData(30).map( _.toString ).mkString(", ") ) )
            logger.info(" ##### KERNEL [%s]: Map Op: combine, axes = %s, result shape = %s, result value[0] = %.4f".format( name, axes, result.getShape.mkString(","), result.getArrayData(1)(0) ) )
            val result_metadata = inputs.metadata ++ input_array.metadata ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements))
            context.operation.rid -> HeapFltArray( result, input_array.origin, result_metadata, None )
          case None =>
            logger.info(" ##### KERNEL [%s]: Map Op: NONE".format( name ) )
            context.operation.rid -> HeapFltArray( input_array.toCDFloatArray, input_array.origin, input_array.metadata, None )
        }
      case None => throw new Exception( "Missing input to '" + this.getClass.getName + "' map op: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(",") )
    }
    val dt = (System.nanoTime - t0) / 1.0E9
    logger.info("Executed Kernel %s map op, time = %.4f s".format(name, dt ))
    context.addTimestamp( "Map Op complete, time = %.4f s, shape = (%s), record mdata = %s".format( dt, shape.mkString(","), inputs.metadata.mkString(";") ) )
    RDDRecord( TreeMap( elem ), inputs.metadata )
  }
}

abstract class DualRDDKernel( options: Map[String,String] ) extends Kernel(options)  {
  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    if( mapCombineOp.isDefined ) {
      val t0 = System.nanoTime
      val input_arrays: List[ArrayBase[Float]] = context.operation.inputs.map(id => inputs.findElements(id)).foldLeft(List[ArrayBase[Float]]())(_ ++ _)
      assert(input_arrays.size > 1, "Missing input(s) to dual input operation " + id + ": required inputs=(%s), available inputs=(%s)".format(context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",")))
      val ma2_input_arrays = input_arrays.map( _.toFastMaskedArray )
      val result_array: CDFloatArray =   ma2_input_arrays(0).merge( ma2_input_arrays(1), mapCombineOp.get ).toCDFloatArray
      val result_metadata = input_arrays.head.metadata ++ inputs.metadata ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements))
      logger.info("Executed Kernel %s map op, time = %.4f s".format(name, (System.nanoTime - t0) / 1.0E9))
      context.addTimestamp("Map Op complete")
      RDDRecord( TreeMap(context.operation.rid -> HeapFltArray(result_array, input_arrays(0).origin, result_metadata, None)), inputs.metadata)
    } else { inputs }
  }
}

//abstract class MultiRDDKernel( options: Map[String,String] ) extends Kernel(options)  {
//
//  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
//    val t0 = System.nanoTime
//    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
//    val async = context.config("async", "false").toBoolean
//    val input_arrays: List[ArrayBase[Float]] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[ArrayBase[Float]]())( _ ++ _ )
//    assert( input_arrays.size > 1, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format( context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",") ) )
//    val cdFloatArrays = input_arrays.map( _.toCDFloatArray ).toArray
//    val final_result: CDFloatArray = if( mapCombineNOp.isDefined ) {
//      CDFloatArray.combine( mapCombineNOp.get, cdFloatArrays )
//    } else if( mapCombineWNOp.isDefined ) {
//      val (result_array, countArray) = CDFloatArray.combine( mapCombineWNOp.get, cdFloatArrays )
//      result_array / countArray
//    } else { throw new Exception("Undefined operation in MultiRDDKernel") }
//    logger.info("&MAP: Finished Kernel %s, time = %.4f s".format(name, (System.nanoTime - t0) / 1.0E9))
//    context.addTimestamp( "Map Op complete" )
//    val result_metadata = input_arrays.head.metadata ++ List( "uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile( inputs.elements )  )
//    RDDRecord( Map( context.operation.rid -> HeapFltArray(final_result, input_arrays(0).origin, result_metadata, None) ), inputs.metadata )
//  }
//}

class CDMSRegridKernel extends zmqPythonKernel( "python.cdmsmodule", "regrid", "Regridder", "Regrids the inputs using UVCDAT", Map( "parallelize" -> "True" ) ) {

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    logger.info("&&MAP&&")
    val t0 = System.nanoTime
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance
    val worker: PythonWorker = workerManager.getPythonWorker
    try {
      val targetGridSpec: String = context.config("gridSpec", inputs.elements.values.head.gridSpec)
      val input_arrays: List[HeapFltArray] = context.operation.inputs.map(id => inputs.findElements(id)).foldLeft(List[HeapFltArray]())(_ ++ _)
      assert(input_arrays.nonEmpty, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format(context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",")))

      val (acceptable_arrays, regrid_arrays) = input_arrays.partition(_.gridSpec.equals(targetGridSpec))
      if (regrid_arrays.isEmpty) {
        logger.info("&MAP: NoOp for Kernel %s".format(name))
        inputs
      } else {
        for (input_array <- acceptable_arrays) { worker.sendArrayMetadata( input_array.uid, input_array) }
        for (input_array <- regrid_arrays)     { worker.sendRequestInput(input_array.uid, input_array) }
        val acceptable_array_map = Map(acceptable_arrays.map(array => array.uid -> array): _*)

        logger.info("Gateway: Executing operation %s".format( context.operation.identifier ) )
        val context_metadata = indexAxisConf(context.getConfiguration, context.grid.axisIndexMap) + ("gridSpec" -> targetGridSpec )
        val rID = UID()
        worker.sendRequest("python.cdmsModule.regrid-" + rID, regrid_arrays.map(_.uid).toArray, context_metadata )

        val resultItems = for (input_array <- regrid_arrays) yield {
          val tvar = worker.getResult
          val result = HeapFltArray( tvar, Some(targetGridSpec) )
          context.operation.rid + ":" + input_array.uid -> result
        }
        val array_metadata = inputs.metadata ++ input_arrays.head.metadata ++ List("uid" -> context.operation.rid, "gridSpec" -> targetGridSpec )
        val array_metadata_crs = context.crsOpt.map( crs => array_metadata + ( "crs" -> crs ) ).getOrElse( array_metadata )
        logger.info("&MAP: Finished Kernel %s, time = %.4f s, metadata = %s".format(name, (System.nanoTime - t0) / 1.0E9, array_metadata_crs.mkString(";")))
        context.addTimestamp( "Map Op complete" )
        RDDRecord(TreeMap(resultItems: _*) ++ acceptable_array_map, array_metadata_crs)
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

  override def cleanUp(): Unit = PythonWorkerPortal.getInstance.shutdown()

  override def map ( context: KernelContext ) ( inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance()
    val worker: PythonWorker = workerManager.getPythonWorker
    try {
      val input_arrays: List[HeapFltArray] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[HeapFltArray]())( _ ++ _ )
      assert( input_arrays.nonEmpty, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format(context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",")))
      val operation_input_arrays = context.operation.inputs.flatMap( input_id => inputs.element( input_id ) )
      val t1 = System.nanoTime
      for( input_id <- context.operation.inputs ) inputs.element(input_id) match {
        case Some( input_array ) =>
          if( addWeights( context ) ) {
            val weights: CDFloatArray = KernelUtilities.getWeights(input_id, context, weightsOpt, false )
            worker.sendRequestInput(input_id, HeapFltArray(input_array, weights))
          } else {
            worker.sendRequestInput(input_id, input_array)
          }
        case None =>
          worker.sendUtility( List( "input", input_id ).mkString(";") )
      }
      val metadata = indexAxisConf( context.getConfiguration, context.grid.axisIndexMap )
      worker.sendRequest(context.operation.identifier, context.operation.inputs.toArray, metadata )
      val resultItems = for( iInput <-  0 until (operation_input_arrays.length * nOutputsPerInput)  ) yield {
        val tvar: TransVar = worker.getResult
        val uid = tvar.getMetaData.get( "uid" )
        val result = HeapFltArray( tvar )
        logger.info( "Received result Var: " + tvar.toString + ", first = " + result.data(0).toString + " undef = " + result.missing.getOrElse(0.0))
        context.operation.rid + ":" + uid + "~" + tvar.id() -> result
      }
      logger.info( "Gateway: Executing operation %s in time %.4f s".format( context.operation.identifier, (System.nanoTime - t1) / 1.0E9 ) )

      val result_metadata = inputs.metadata ++ input_arrays.head.metadata ++ List( "uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile( inputs.elements )  )
      logger.info("&MAP: Finished zmqPythonKernel %s, time = %.4f s, metadata = %s".format(name, (System.nanoTime - t0) / 1.0E9, result_metadata.mkString(";") ) )
      context.addTimestamp( "Map Op complete" )
      RDDRecord( TreeMap(resultItems:_*), result_metadata )
    } finally {
      workerManager.releaseWorker( worker )
    }
  }

  override def customReduceRDD(context: KernelContext)(a0: ( RecordKey, RDDRecord ), a1: ( RecordKey, RDDRecord ) ): ( RecordKey, RDDRecord ) = {
    val ( rdd0, rdd1 ) = ( a0._2, a1._2 )
    val ( k0, k1 ) = ( a0._1, a1._1 )
    val t0 = System.nanoTime
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance
    val worker: PythonWorker = workerManager.getPythonWorker
    val ascending = k0 < k1
    val new_key = if(ascending) { k0 + k1 } else { k1 + k0 }
    val op_metadata = indexAxisConf( context.getConfiguration, context.grid.axisIndexMap )
    rdd0.elements.map {
      case (key, element0) =>  rdd1.elements.get(key).map( element1 => key -> {
        val (array0, array1) = if (ascending) (element0, element1) else (element1, element0)
        val uids = Array( s"${array0.uid}", s"${array1.uid}" )
        worker.sendRequestInput( uids(0), array0 )
        worker.sendRequestInput( uids(1), array1 )
        worker.sendRequest( context.operation.identifier, uids, Map( "action" -> "reduce", "axes" -> context.getAxes.getAxes.mkString(",") ) )
      })
    }
    val resultItems = rdd0.elements.map {
      case (key, element0) =>
        val tvar = worker.getResult
        val result = HeapFltArray( tvar )
        context.operation.rid + ":" + element0.uid -> result
    }
    logger.debug("&MERGE %s: finish, time = %.4f s".format( context.operation.identifier, (System.nanoTime - t0) / 1.0E9 ) )
    context.addTimestamp( "Custom Reduce Op complete" )
    new_key -> RDDRecord( resultItems, rdd0.mergeMetadata("merge", rdd1) )
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
