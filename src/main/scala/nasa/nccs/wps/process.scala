package nasa.nccs.wps

abstract class WPSElement {
  val identifier: String
  val title: String = ""
  val description: String = ""
  val keywords: List[String] = List()

  def getHeader: List[xml.Node] = List(
    <ows:Identifier>{identifier}</ows:Identifier>,
    <ows:Title>{title}</ows:Title>,
    <ows:Abstract>{description}</ows:Abstract> )
}

class WPSDataInput( _id:String,  val minoccurs: Int, val maxoccurs: Int, _title: String="", _abstract : String="" ) extends WPSElement {
  val identifier = _id
  override val title = _title
  override val description = _abstract
  val Describe: xml.Elem = <Input minOccurs={minoccurs.toString} maxOccurs={maxoccurs.toString}> {getHeader} </Input>
}

class WPSProcessOutput( _id:String,  val mimeType: String, _title: String="", _abstract : String="" ) extends WPSElement {
  val identifier = _id
  override val title = _title
  override val description = _abstract
  val Describe: xml.Elem =
    <Output> {getHeader}
    <ComplexOutput>
      <Default> <Format> <MimeType>{mimeType}</MimeType> </Format> </Default>
      <Supported> <Format> <MimeType>{mimeType}</MimeType> </Format> </Supported>
    </ComplexOutput>
  </Output>
}

abstract class WPSProcess extends WPSElement {
  val inputs: List[WPSDataInput]
  val outputs: List[WPSDataInput]

  def GetCapabilities(): xml.Node = <wps:Process wps:processVersion="1"> {getHeader} </wps:Process>

  def DescribeProcess: xml.Node =
    <ProcessDescription wps:processVersion="2" storeSupported="true" statusSupported="false">
      {getHeader}
      <DataInputs> { inputs.map( _.Describe ) } </DataInputs>
      <ProcessOutputs> { outputs.map( _.Describe ) }</ProcessOutputs>
    </ProcessDescription>
}


object TestProcess extends WPSProcess {
  val identifier: String = "server-id"
  override val title: String = "server-abstract"
  override val description: String = "server-title"

  override val inputs: List[WPSDataInput] = List( new WPSDataInput("input-0",1,1,"input-title","input-abstract") )
  override val outputs: List[WPSProcessOutput] = List( new WPSProcessOutput("output-0","text/xml","output-title","output-abstract") )
}

object WPSTest extends App {
  val printer = new scala.xml.PrettyPrinter(200, 3)
  val response = TestProcess.DescribeProcess
  println( printer.format(response) )
}
