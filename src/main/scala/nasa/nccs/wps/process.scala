package nasa.nccs.wps

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.utilities.appParameters

trait WPSElement extends Serializable {
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

class WPSDataInput( _id:String,  val minoccurs: Int, val maxoccurs: Int, _title: String, _abstract : String,  _keywords : List[String] ) extends WPSElement  {
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

class WPSWorkflowProcess( val identifier: String, val description: String, val title: String, val inputs: List[WPSDataInput],  val outputs: List[WPSProcessOutput] ) extends WPSProcess {

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
