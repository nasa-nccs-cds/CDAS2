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

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await}

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

class CDASExecutionContext( val operation: OperationContext, val request: RequestContext, val server: ServerContext ) {}

trait ExecutionResult {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def toXml: xml.Elem
}

class UtilityExecutionResult( val id: String, val response: xml.Elem )  extends ExecutionResult {
  def toXml = <result id={id}> {response} </result>
}
class BlockingExecutionResult( val id: String, val intputSpecs: List[DataFragmentSpec], val gridSpec: TargetGrid, val result_tensor: CDFloatArray ) extends ExecutionResult {
  def toXml = {
    val idToks = id.split('~')
    logger.info( "BlockingExecutionResult-> result_tensor: \n" + result_tensor.toString )
    <result id={idToks(1)} op={idToks(0)}> { intputSpecs.map( _.toXml ) } { gridSpec.toXml } <data undefined={result_tensor.getInvalid.toString}> {result_tensor.mkDataString(",")}  </data>  </result>
  }
}

class ErrorExecutionResult( val err: Throwable ) extends ExecutionResult {

  def fatal(): String = {
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    err.getMessage
  }

  def toXml = <error> {fatal()} </error>

}

class XmlExecutionResult( val id: String,  val responseXml: xml.Node ) extends ExecutionResult {
  def toXml = {
    val idToks = id.split('~')
    <result id={idToks(1)} op={idToks(0)}> { responseXml }  </result>
  }
}

// cdsutils.cdata(

class AsyncExecutionResult( val results: List[String] )  extends ExecutionResult  {
  def this( resultOpt: Option[String]  ) { this( resultOpt.toList ) }
  def toXml = <result> {  results.mkString(",")  } </result>
}

class ExecutionResults( val results: List[ExecutionResult] ) {
  def this(err: Throwable ) = this( List( new ErrorExecutionResult( err ) ) )
  def toXml = <results> { results.map(_.toXml) } </results>
}

case class ResultManifest( val name: String, val dataset: String, val description: String, val units: String )

//class SingleInputExecutionResult( val operation: String, manifest: ResultManifest, result_data: Array[Float] ) extends ExecutionResult(result_data) {
//  val name = manifest.name
//  val description = manifest.description
//  val units = manifest.units
//  val dataset =  manifest.dataset
//
//  override def toXml =
//    <operation id={ operation }>
//      <input name={ name } dataset={ dataset } units={ units } description={ description }  />
//      { super.toXml }
//    </operation>
//}


class AxisIndices( private val axisIds: Set[Int] = Set.empty ) {
  def getAxes: Seq[Int] = axisIds.toSeq
  def args = axisIds.toArray
  def includes( axisIndex: Int ): Boolean = axisIds.contains( axisIndex )
}
// , val binArrayOpt: Option[BinnedArrayFactory], val dataManager: DataManager, val serverConfiguration: Map[String, String], val args: Map[String, String],

//class ExecutionContext( operation: OperationContext, val domains: Map[String,DomainContainer], val dataManager: DataManager ) {
//
//
//  def id: String = operation.identifier
//  def getConfiguration( cfg_type: String ): Map[String,String] = operation.getConfiguration( cfg_type )
//  def binArrayOpt = dataManager.getBinnedArrayFactory( operation )
//  def inputs: List[PartitionedFragment] = for( uid <- operation.inputs ) yield new PartitionedFragment( dataManager.getVariableData(uid), dataManager.getAxisIndices(uid) )
//
//
//  //  def getSubset( var_uid: String, domain_id: String ) = {
////    dataManager.getSubset( var_uid, getDomain(domain_id) )
////  }
//  def getDataSources: Map[String,DataFragmentSpec] = dataManager.getDataSources
//
//  def async: Boolean = getConfiguration("run").getOrElse("async", "false").toBoolean
//
//  def getFragmentSpec( uid: String ): DataFragmentSpec = dataManager.getOperationInputSpec(uid) match {
//    case None => throw new Exception( "Missing Data Fragment Spec: " + uid )
//    case Some( inputSpec ) => inputSpec.data
//  }
//
//  def getAxisIndices( uid: String ): AxisIndices = dataManager.getAxisIndices( uid )
//}

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

  val combineOp: ReduceOpFlt  = ( x, y ) => { math.max(x,y) }
  val initValue: Float = 0f

  def mapReduce( inputs: List[PartitionedFragment], context: CDASExecutionContext, nprocs: Int ): Future[DataFragment]

  def execute( context: CDASExecutionContext, nprocs: Int  ): ExecutionResult = {
    logger.info( s"Kernel[$name($id)].execute: " + context.operation.toString )
    val inputs: List[PartitionedFragment] = inputVars( context )
    var opResult: Future[DataFragment] = mapReduce( inputs, context, nprocs )
    createResponse( postOp( opResult, context  ), inputs, context )
  }
  def postOp( future_result: Future[DataFragment], context: CDASExecutionContext ):  Future[DataFragment] = future_result
  def reduce( future_results: IndexedSeq[Future[DataFragment]], context: CDASExecutionContext ):  Future[DataFragment] = Future.reduce(future_results)(reduceOp(context) _)

  def reduceOp( context: CDASExecutionContext )( a0: DataFragment, a1: DataFragment ): DataFragment = {
    val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
    if( axes.includes(0) )  { new DataFragment( a0.spec, CDFloatArray.combine( combineOp, a0.data, a1.data ) ) }
    else                    {  a0 ++ a1  }
  }
  def createResponse( resultFut: Future[DataFragment], inputs: List[PartitionedFragment], context: CDASExecutionContext ): ExecutionResult = {
    val inputVar: PartitionedFragment = inputs.head
    val async = context.request.config("async", "false").toBoolean
    if(async) {
      new AsyncExecutionResult( cacheResult( resultFut, context, inputVar.getVariableMetadata(context.server) ) )
    } else {
      val result: DataFragment = Await.result( resultFut, Duration.Inf )
      new BlockingExecutionResult(context.operation.identifier, List(inputVar.fragmentSpec), context.request.targetGrid.getSubGrid(result.spec.roi), result.data )
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

  def inputVars( context: CDASExecutionContext ): List[PartitionedFragment] = context.server.inputs( context.operation.inputs.map( context.request.getInputSpec ) )

  def cacheResult( resultFut: Future[DataFragment], context: CDASExecutionContext, varMetadata: Map[String,nc2.Attribute] ): Option[String] = {
    try {
      val tFragFut = resultFut.map( dataFrag =>  new TransientFragment( dataFrag, context.request, varMetadata ) )
      collectionDataCache.putResult( context.operation.rid, tFragFut )
      Some(context.operation.rid)
    } catch {
      case ex: Exception => logger.error( "Can't cache result: " + ex.getMessage ); None
    }
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

abstract class SingularKernel extends Kernel {

  def mapReduce( inputs: List[PartitionedFragment], context: CDASExecutionContext, nprocs: Int ): Future[DataFragment] = {
    val future_results: IndexedSeq[Future[DataFragment]] = (0 until nprocs).map( iproc => Future { map(iproc,inputs,context) } )
    reduce( future_results, context )
  }
  def map( partIndex: Int, inputs: List[PartitionedFragment], context: CDASExecutionContext ): DataFragment = {
    logger.info("SingularKernel(%s).map[%d] ".format( name, partIndex ))
    val inputVar = inputs.head
    val axes: AxisIndices = context.request.getAxisIndices( context.operation.config("axes","") )
    val dataFrag: DataFragment = inputVar.domainDataFragment(partIndex)
    val async = context.request.config("async", "false").toBoolean
    val resultFragSpec = dataFrag.getReducedSpec( axes )
    val t10 = System.nanoTime
    val result_val_masked: CDFloatArray = dataFrag.data.reduce( combineOp, axes.args, initValue )
    val t11 = System.nanoTime
    logger.info("Executed Kernel %s[%d] map op, time = %.4f s".format( name, partIndex, (t11-t10)/1.0E9 ) )
    new DataFragment( resultFragSpec, result_val_masked )
  }

}

class KernelModule {
  val logger = LoggerFactory.getLogger(this.getClass)
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  logger.info( "---> new KernelModule: " + identifiers.mkString(", ") )
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

class TransientFragment( val dataFrag: DataFragment, val request: RequestContext, val varMetadata: Map[String,nc2.Attribute] ) extends Loggable {
  def toXml(id: String): xml.Elem = {
    val units = varMetadata.get("units") match { case Some(attr) => attr.getStringValue; case None => "" }
    val long_name = varMetadata.getOrElse("long_name",varMetadata.getOrElse("fullname",varMetadata.getOrElse("varname", new Attribute("varname","UNDEF")))).getStringValue
    val description = varMetadata.get("description") match { case Some(attr) => attr.getStringValue; case None => "" }
    val axes = varMetadata.get("axes") match { case Some(attr) => attr.getStringValue; case None => "" }
    <result id={id} missing_value={dataFrag.data.getInvalid.toString} shape={dataFrag.data.getShape.mkString("(",",",")")} units={units} long_name={long_name} description={description} axes={axes}> { dataFrag.data.mkBoundedDataString( ", ", 1100 ) } </result> //
  }

}

//object classTest extends App {
//  import nasa.nccs.cds2.modules.CDS._
//  printf( Kernel.getClass.isAssignableFrom( CDS. ).toString )
//}

