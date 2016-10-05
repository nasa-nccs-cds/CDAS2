package nasa.nccs.wps

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.utilities.appParameters

trait WPSElement {
  val identifier: String
  val title: String
  val description: String
  val keywords: List[String] = List.empty

  def getHeader: List[xml.Node] = List(
    <ows:Identifier>{identifier}</ows:Identifier>,      <ows:Title>{title}</ows:Title>,
    <ows:Abstract>{description}</ows:Abstract> )   ++   keywords.map( kw => <ows:Metadata xlink:title={kw} /> )
}

object WPSDataInput {
  def apply( _id:String,  minoccurs: Int, maxoccurs: Int, _title: String="", _abstract: String="", _keywords: List[String] = List.empty ) = new WPSDataInput( _id, minoccurs, maxoccurs, _title, _abstract, _keywords )
}

class WPSDataInput( _id:String,  val minoccurs: Int, val maxoccurs: Int, _title: String, _abstract : String,  _keywords : List[String] ) extends WPSElement {
  val identifier = _id
  val title = _title
  val description = _abstract
  override val keywords = _keywords
  val Describe: xml.Elem = <wps:Input minOccurs={minoccurs.toString} maxOccurs={maxoccurs.toString}> {getHeader} </wps:Input>
}

object WPSProcessOutput {
  def apply( _id:String,  mimeType: String="text/xml", _title: String="", _abstract: String="", _keywords: List[String] = List.empty ) = new WPSProcessOutput( _id, mimeType, _title, _abstract, _keywords )
}

class WPSProcessOutput( _id:String,  val mimeType: String, _title: String, _abstract : String,  _keywords: List[String] ) extends WPSElement {
  val identifier = _id
  val title = _title
  val description = _abstract
  override val keywords = _keywords

  val Describe: xml.Elem =
    <wps:Output> {getHeader}
    <ComplexOutput>
      <Default> <Format> <MimeType>{mimeType}</MimeType> </Format> </Default>
      <Supported> <Format> <MimeType>{mimeType}</MimeType> </Format> </Supported>
    </ComplexOutput>
  </wps:Output>
}

object WPSExecuteResponse {
  def merge( responses: List[WPSExecuteResponse] ): WPSExecuteResponse = new MergedWPSExecuteResponse( responses )
}

trait WPSResponse {
  def toXml: xml.Elem
}

abstract class WPSExecuteResponse( val processes: List[WPSProcess] ) extends WPSResponse {
  val serviceInstance = appParameters("wps.server.result.href","")
  val statusLocation =  appParameters("wps.server.status.href","")
  def this( process: WPSProcess ) = this( List(process) )

  def toXml: xml.Elem =
    <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" serviceInstance={serviceInstance} statusLocation={statusLocation}>
      { processes.map( _.ExecuteHeader ) }
      <wps:ProcessOutputs> { getOutputs } </wps:ProcessOutputs>
    </wps:ExecuteResponse>

  def getOutputs: List[xml.Elem] = processes.flatMap( p => p.outputs.map( output => <wps:Output> {output.getHeader} { getProcessOutputs( p.identifier, output.identifier) } </wps:Output> ) )
  def getProcessOutputs( process_id: String, output_id: String ): Iterable[xml.Elem]
  def getData( id: String, array: CDFloatArray, units: String ): xml.Elem = <wps:Data id={id}> <wps:LiteralData uom={units} shape={array.getShape}>{ array.toDataString }</wps:LiteralData> </wps:Data>
}

abstract class WPSReferenceExecuteResponse( val process: WPSProcess, val optResultId: Option[String] )  extends WPSExecuteResponse(process)  {

  val result_href = serviceInstance + "/" +  optResultId.getOrElse("")
  def getReference( process_id: String, output_id: String ): xml.Elem = <wps:Reference href={result_href} mimeType="text/xml"/>
}

class MergedWPSExecuteResponse( responses: List[WPSExecuteResponse] ) extends WPSExecuteResponse( responses.flatMap(_.processes) ) {
  val process_ids: List[String] = responses.flatMap( response => response.processes.map( process => process.identifier ) )
  assert( process_ids.distinct.size == process_ids.size, "Error, non unique process IDs in process list: " + processes.mkString(", ") )
  val responseMap: Map[String,WPSExecuteResponse] = Map( responses.flatMap( response => response.processes.map( process => ( process.identifier -> response ) ) ): _* )
  def getProcessOutputs( process_id: String, response_id: String ): Iterable[xml.Elem] = responseMap.get( process_id ) match {
      case Some( response ) => response.getProcessOutputs(process_id, response_id);
      case None => throw new Exception( "Unrecognized process id: " + process_id )
    }
}

trait WPSProcess extends WPSElement {
  val inputs: List[WPSDataInput]
  val outputs: List[WPSProcessOutput]

  def GetCapabilities(): xml.Elem = <wps:Process wps:processVersion="1"> {getHeader} </wps:Process>

  def DescribeProcess: xml.Elem =
    <wps:ProcessDescription wps:processVersion="2" storeSupported="true" statusSupported="false">
      {getHeader}
      <wps:DataInputs> { inputs.map( _.Describe ) } </wps:DataInputs>
      <wps:ProcessOutputs> { outputs.map( _.Describe ) }</wps:ProcessOutputs>
    </wps:ProcessDescription>

  def ExecuteHeader: xml.Elem = <wps:Process wps:processVersion="1"> {getHeader} </wps:Process>

}


object TestProcess extends WPSProcess {
  val identifier = "server-id"
  val title = "server-abstract"
  val description = "server-title"
  override val keywords = List.empty

  val inputs = List( WPSDataInput("input-0",1,1,"input-title","input-abstract") )
  val outputs = List( WPSProcessOutput("output-0","text/xml","output-title","output-abstract") )
}

object WPSTest extends App {
  val printer = new scala.xml.PrettyPrinter(200, 3)
  val response = TestProcess.DescribeProcess
  println( printer.format(response) )
}
