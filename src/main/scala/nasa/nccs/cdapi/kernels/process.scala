package nasa.nccs.cdapi.kernels

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdapi.cdm._
import nasa.nccs.esgf.process._
import org.slf4j.LoggerFactory
import java.io.{File, IOException, PrintWriter, StringWriter}

import ucar.{ma2, nc2}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

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

trait ExecutionResult {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def toXml: xml.Elem
}

class BlockingExecutionResult( val id: String, val intputSpecs: List[DataFragmentSpec], val gridSpec: GridSpec, val result_tensor: CDFloatArray ) extends ExecutionResult {
  def toXml = {
    val idToks = id.split('~')
    logger.info( "BlockingExecutionResult-> result_tensor: \n" + result_tensor.toString )
    <result id={idToks(1)} op={idToks(0)}> { intputSpecs.map( _.toXml ) } { gridSpec.toXml } <data undefined={result_tensor.getInvalid.toString}> {result_tensor.copySectionData.mkString(",")}  </data>  </result>
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
}
// , val binArrayOpt: Option[BinnedArrayFactory], val dataManager: DataManager, val serverConfiguration: Map[String, String], val args: Map[String, String],

//class ExecutionContext( operation: OperationContext, val domains: Map[String,DomainContainer], val dataManager: DataManager ) {
//
//
//  def id: String = operation.identifier
//  def getConfiguration( cfg_type: String ): Map[String,String] = operation.getConfiguration( cfg_type )
//  def binArrayOpt = dataManager.getBinnedArrayFactory( operation )
//  def inputs: List[KernelDataInput] = for( uid <- operation.inputs ) yield new KernelDataInput( dataManager.getVariableData(uid), dataManager.getAxisIndices(uid) )
//
//
//  //  def getSubset( var_uid: String, domain_id: String ) = {
////    dataManager.getSubset( var_uid, getDomain(domain_id) )
////  }
//  def getDataSources: Map[String,OperationInputSpec] = dataManager.getDataSources
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
    val resultsDirPath = serverConfiguration.getOrElse("wps.results.dir", System.getProperty("user.home") + "/.wps/results")
    val resultsDir = new File(resultsDirPath); resultsDir.mkdirs()
    val resultFile = new File( resultsDirPath + s"/$resultId.nc" )
    if( deleteExisting && resultFile.exists ) resultFile.delete
    resultFile
  }
}

abstract class Kernel {
  val logger = LoggerFactory.getLogger(this.getClass)
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

  def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext   ): ExecutionResult
  def toXmlHeader =  <kernel module={module} name={name}> { if (description.nonEmpty) <description> {description} </description> } </kernel>

  def toXml = {
    <kernel module={module} name={name}>
      {if (description.nonEmpty) <description>{description}</description> }
      {if (keywords.nonEmpty) <keywords> {keywords.mkString(",")} </keywords> }
      {if (identifier.nonEmpty) <identifier> {identifier} </identifier> }
      {if (metadata.nonEmpty) <metadata> {metadata} </metadata> }
    </kernel>
  }

  def inputVars( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): List[KernelDataInput] = serverCx.inputs(operationCx.inputs.map( requestCx.getInputSpec(_) ) )

  def searchForValue( metadata: Map[String,nc2.Attribute], keys: List[String], default_val: String ) : String = {
    keys.length match {
      case 0 => default_val
      case x => metadata.get(keys.head) match {
        case Some(valueAttr) => valueAttr.getStringValue()
        case None => searchForValue(metadata, keys.tail, default_val)
      }
    }
  }
//
  def saveResult( maskedTensor: CDFloatArray, request: RequestContext, server: ServerContext, gridSpec: GridSpec, varMetadata: Map[String,nc2.Attribute], dsetMetadata: List[nc2.Attribute] ): Option[String] = {
    request.config("resultId") match {
      case None => logger.warn("Missing resultId: this probably means you are executing synchronously with 'async' = true ")
      case Some(resultId) =>
        val inputSpec = request.getInputSpec()
        val dataset: CDSDataset = request.getDataset(server)
        val varname = searchForValue( varMetadata, List("varname","fullname","standard_name","original_name","long_name"), "Nd4jMaskedTensor" )
        val resultFile = Kernel.getResultFile( server.getConfiguration, resultId, true )
        val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, resultFile.getAbsolutePath )
        assert(gridSpec.axes.length == maskedTensor.getRank, "Axes not the same length as data shape in saveResult")
        val coordAxes = dataset.getCoordinateAxes
        val dims: IndexedSeq[nc2.Dimension] = (0 until gridSpec.axes.length).map( idim => writer.addDimension(null, gridSpec.axes(idim).name, maskedTensor.getShape(idim)))
        val newCoordVars: List[ (nc2.Variable,ma2.Array) ] = ( for( coordAxis <- coordAxes ) yield inputSpec.getRange( coordAxis.getShortName ) match {
          case Some( range ) =>
            val coordVar: nc2.Variable = writer.addVariable( null, coordAxis.getShortName, coordAxis.getDataType, coordAxis.getShortName )
            for( attr <- coordAxis.getAttributes ) writer.addVariableAttribute( coordVar, attr )
            Some( coordVar, coordAxis.read( List(range) ) )
          case None => None
        } ).flatten
        val variable: nc2.Variable = writer.addVariable(null, varname, ma2.DataType.FLOAT, dims.toList)
        varMetadata.values.foreach( attr => variable.addAttribute(attr) )
        variable.addAttribute( new nc2.Attribute( "missing_value", maskedTensor.getInvalid ) )
        dsetMetadata.foreach( attr => writer.addGroupAttribute(null, attr ) )
        try {
          writer.create()
          for( newCoordVar <- newCoordVars ) newCoordVar match { case ( coordVar, coordData ) =>  writer.write( coordVar, coordData ) }
          writer.write( variable, maskedTensor )
//          for( dim <- dims ) {
//            val dimvar: nc2.Variable = writer.addVariable(null, dim.getFullName, ma2.DataType.FLOAT, List(dim) )
//            writer.write( dimvar, dimdata )
//          }
          writer.close()
          println( "Writing result %s to file '%s'".format(resultId,resultFile.getAbsolutePath) )
          Some(resultId)
        } catch {
          case e: IOException => logger.error("ERROR creating file %s%n%s".format(resultFile.getAbsolutePath, e.getMessage()))
        }
    }
    None
  }

  //  def binArrayOpt = serverContext.getBinnedArrayFactory( operation )

  //
  //
  //  //  def getSubset( var_uid: String, domain_id: String ) = {
  ////    serverContext.getSubset( var_uid, getDomain(domain_id) )
  ////  }
  //  def getDataSources: Map[String,OperationInputSpec] = serverContext.getDataSources
  //
  //
  //
  //  def getFragmentSpec( uid: String ): DataFragmentSpec = serverContext.getOperationInputSpec(uid) match {
  //    case None => throw new Exception( "Missing Data Fragment Spec: " + uid )
  //    case Some( inputSpec ) => inputSpec.data
  //  }
  //
  //  def getAxisIndices( uid: String ): AxisIndices = serverContext.getAxisIndices( uid )

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

  def getKernelClasses = getInnerClasses.filter( _.getSuperclass.getName.split('.').last == "Kernel"  )
  def getInnerClasses = this.getClass.getClasses.toList
  def getKernelObjects = getKernelClasses.map( _.getDeclaredConstructors()(0).newInstance(this).asInstanceOf[Kernel] )

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

