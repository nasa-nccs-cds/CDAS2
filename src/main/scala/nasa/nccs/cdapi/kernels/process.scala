package nasa.nccs.cdapi.kernels

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdapi.cdm._
import nasa.nccs.esgf.process._
import org.slf4j.LoggerFactory
import java.io.{File, IOException, PrintWriter, StringWriter}

import scala.concurrent.ExecutionContext.Implicits.global
import nasa.nccs.caching.collectionDataCache
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import nasa.nccs.utilities.Loggable
import ucar.nc2.Attribute
import ucar.{ma2, nc2}

import scala.util.{ Random, Success, Failure }
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

class Port( val name: String, val cardinality: String, val description: String, val datatype: String, val identifier: String )  {

  def toXml = {
    <port name={name} cardinality={cardinality}>
      { if ( description.nonEmpty ) <description> {description} </description> }
      { if ( datatype.nonEmpty ) <datatype> {datatype} </datatype> }
      { if ( identifier.nonEmpty ) <identifier> {identifier} </identifier> }
    </port>
  }
}

class CDASExecutionContext( val operation: OperationContext, val request: RequestContext, val server: ServerContext ) extends Loggable {

  def getOpSections: Option[ IndexedSeq[ma2.Section] ] = {
    val optargs: Map[String, String] = operation.getConfiguration
    val domains: IndexedSeq[DomainContainer] = optargs.get("domain") match {
      case Some(domainIds) => domainIds.split(",").map(request.getDomain(_))
      case None => return Some( IndexedSeq.empty[ma2.Section] )
    }
    logger.info( "OPT DOMAIN Arg: " + optargs.getOrElse( "domain", "None" ) )
    logger.info( "OPT Domains: " + domains.map(_.toString).mkString( ", " ) )
    Some( domains.map(dc => request.targetGrid.grid.getSubSection(dc.axes) match {
      case Some(section) => section
      case None => return None
    }))
  }
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
    logger.info( "BlockingExecutionResult-> result_tensor: \n" + result_tensor.toString )
    val inputs = intputSpecs.map( _.toXml )
    val grid = gridSpec.toXml
    val results = result_tensor.mkDataString(",")
    <result id={id} op={idToks.head} rid={resultId.getOrElse("")}> { inputs } { grid } <data undefined={result_tensor.getInvalid.toString}> {results}  </data>  </result>
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

abstract class Kernel extends Loggable {
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

  def mapReduce(context: CDASExecutionContext, nprocs: Int): Future[Option[DataFragment]] = {
    val opInputs: List[OperationInput] = for ( uid <- context.operation.inputs ) yield {
      context.request.getInputSpec(uid) match {
        case Some(inputSpec) =>  { context.server.getOperationInput(inputSpec) }
        case None => collectionDataCache.getExistingResult( uid ) match {
          case Some( tFragFut ) => Await result ( tFragFut, Duration.Inf )
          case None => throw new Exception( "Unrecognized input id: " + uid )
        }
      }
    }
    val future_results: IndexedSeq[Future[Option[DataFragment]]] = ( 0 until nprocs ) map (
      iproc => Future { map ( iproc, opInputs map ( _.domainDataFragment( iproc, context ) ), context ) }
    )
    reduce(future_results, context)
  }

  def map( partIndex: Int, inputs: List[Option[DataFragment]], context: CDASExecutionContext ): Option[DataFragment] = { inputs.head }

  def executeProcess( context: CDASExecutionContext, nprocs: Int  ): ExecutionResult = {
    val t0 = System.nanoTime()
    var opResult: Future[Option[DataFragment]] = mapReduce( context, nprocs )
    opResult.onComplete {
      case Success(dataFragOpt) =>
        logger.info(s"********** Completed Execution of Kernel[$name($id)]: %s , total time = %.3f sec  ********** \n".format(context.operation.toString, (System.nanoTime() - t0) / 1.0E9))
      case Failure(t) =>
        logger.error(s"********** Failed Execution of Kernel[$name($id)]: %s ********** \n".format(context.operation.toString ))
        logger.error( " ---> Cause: " + t.getCause.getMessage )
        logger.error( "\n" + t.getCause.getStackTrace.mkString("\n") + "\n" )
    }
    createResponse( postOp( opResult, context  ), context )
  }
  def postOp( future_result: Future[Option[DataFragment]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = future_result
  def reduce( future_results: IndexedSeq[Future[Option[DataFragment]]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = Future.reduce(future_results)(reduceOp(context) _)

  def combine(context: CDASExecutionContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment = reduceCombineOpt match {
    case Some(combineOp) =>
      if (axes.includes(0)) new DataFragment(a0.spec, CDFloatArray.combine(combineOp, a0.data, a1.data))
      else { a0 ++ a1 }
    case None => {
      a0 ++ a1
    }
  }

  def reduceOp(context: CDASExecutionContext)(a0op: Option[DataFragment], a1op: Option[DataFragment]): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.request.getAxisIndices(context.operation.config("axes", ""))
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
  def createResponse( resultFut: Future[Option[DataFragment]], context: CDASExecutionContext ): ExecutionResult = {
    val var_mdata = Map[String,Attribute]()
    val async = context.request.config("async", "false").toBoolean
    val resultId = cacheResult( resultFut, context, var_mdata /*, inputVar.getVariableMetadata(context.server) */ )
    if(async) {
      new AsyncExecutionResult( resultId )
    } else {
      val resultOpt: Option[DataFragment] = Await.result( resultFut, Duration.Inf )
      resultOpt match {
        case Some(result) =>
          new BlockingExecutionResult (context.operation.identifier, List(result.spec), context.request.targetGrid.getSubGrid (result.spec.roi), result.data, resultId )
        case None =>
          logger.error( "Operation %s returned empty result".format( context.operation.identifier ) )
          new BlockingExecutionResult (context.operation.identifier, List(), context.request.targetGrid, CDFloatArray.empty )
      }
    }
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

  def cacheResult( resultFut: Future[Option[DataFragment]], context: CDASExecutionContext, varMetadata: Map[String,nc2.Attribute] ): Option[String] = {
    try {
      val tOptFragFut = resultFut.map( dataFragOpt => dataFragOpt.map( dataFrag => new TransientFragment( dataFrag, context.request, varMetadata ) ) )
      collectionDataCache.putResult( context.operation.rid, tOptFragFut )
      Some(context.operation.rid)
    } catch {
      case ex: Exception => logger.error( "Can't cache result: " + ex.getMessage ); None
    }
  }

  def weightedValueSumCombiner(context: CDASExecutionContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices ): DataFragment =  {
    if ( axes.includes(0) ) {
      val vTot = a0.data + a1.data
      val wTot = a0.optData.map( w => w + a1.optData.get )
      new DataFragment( a0.spec, vTot, wTot, DataFragment.combineCoordMaps(a0,a1) )
    }
    else { a0 ++ a1 }
  }

  def weightedValueSumPostOp( future_result: Future[Option[DataFragment]], context: CDASExecutionContext ):  Future[Option[DataFragment]] = {
    future_result.map( _.map( (result: DataFragment) => result.optData match {
      case Some( weights_sum ) =>
//        logger.info( "weightedValueSumPostOp, values = %s, weights = %s".format( result.data.toDataString, weights_sum.toDataString ) )
        new DataFragment( result.spec, result.data / weights_sum, result.optData, result.optCoordMap )
      case None => result
    } ) )
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
      val async = context.request.config("async", "false").toBoolean
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
  override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: CDASExecutionContext ): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
    inputs.head.map( dataFrag => {
      val async = context.request.config("async", "false").toBoolean
      val resultFragSpec = dataFrag.getReducedSpec(axes)
      val result_val_masked: CDFloatArray = mapCombineOpt match {
        case Some(combineOp) => dataFrag.data.reduce(combineOp, axes.args, initValue)
        case None => dataFrag.data
      }
      logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, partIndex, (System.nanoTime - t0) / 1.0E9))
      new DataFragment(resultFragSpec, result_val_masked)
    } )
  }
}

abstract class DualKernel extends Kernel {
  override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: CDASExecutionContext ): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
    assert( inputs.length > 1, "Missing input(s) to dual input operation " + id )
    inputs(0).flatMap( dataFrag0 => {
      inputs(1).map( dataFrag1 => {
        val async = context.request.config("async", "false").toBoolean
        val result_val_masked: DataFragment = mapCombineOpt match {
          case Some(combineOp) => DataFragment.combine( combineOp, dataFrag0, dataFrag1 )
          case None => dataFrag0
        }
        logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format(name, partIndex, (System.nanoTime - t0) / 1.0E9))
        result_val_masked
      })
    })
  }
}

class KernelModule {
  val logger = LoggerFactory.getLogger(this.getClass)
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  def package_path = identifiers.dropRight(1).mkString(".")
  def name: String = identifiers.last
  val version = ""
  val organization = ""
  val author = ""
  val contact = ""
  val kernelMap: Map[String,Kernel] = Map(getKernelObjects.map( kernel => kernel.operation.toLowerCase -> kernel ): _*)

  def getKernelClasses = getInnerClasses // .filter( Kernel.getClass.isAssignableFrom( _ )  )
  def getInnerClasses = this.getClass.getClasses.toList
  def getKernelObjects: List[Kernel] = getKernelClasses.map( _.getDeclaredConstructors()(0).newInstance(this).asInstanceOf[Kernel] )

  def getKernel( kernelName: String ): Option[Kernel] = kernelMap.get( kernelName.toLowerCase )
  def getKernelNames: List[String] = kernelMap.keys.toList

  def toXml = {
    <kernelModule name={name}>
      { if ( version.nonEmpty ) <version> {version} </version> }
      { if ( organization.nonEmpty ) <organization> {organization} </organization> }
      { if ( author.nonEmpty ) <author> {author} </author> }
      { if ( contact.nonEmpty ) <contact> {contact} </contact> }
      <kernels> { kernelMap.values.map( _.toXmlHeader ) } </kernels>
    </kernelModule>
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
  def domainDataFragment( partIndex: Int, context: CDASExecutionContext ): Option[DataFragment] = Some(dataFrag)
  def data(partIndex: Int ): CDFloatArray = dataFrag.data
  def delete() = {;}
}

//object classTest extends App {
//  import nasa.nccs.cds2.modules.CDS._
//  printf( Kernel.getClass.isAssignableFrom( CDS. ).toString )
//}

