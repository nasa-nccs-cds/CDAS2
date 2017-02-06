package nasa.nccs.wps

import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdas.utilities.appParameters

trait WPSElement extends Serializable {
  val identifier: String
  val title: String
  val description: String
  val keywords: List[String] = List.empty

  def getHeader(syntax: ResponseSyntax.Value): List[xml.Node] = syntax match {
    case ResponseSyntax.WPS =>
      List(<ows:Identifier>
        {identifier}
      </ows:Identifier>, <ows:Title>
        {title}
      </ows:Title>,
        <ows:Abstract>
          {description}
        </ows:Abstract>) ++ keywords.map(kw => <ows:Metadata xlink:title={kw}/>)
    case ResponseSyntax.Generic =>
      List(<description id={identifier} title={title} keywords={keywords.mkString(",")}>
        {description}
      </description>)
  }
}

object WPSDataInput {
  def apply(_id: String, minoccurs: Int, maxoccurs: Int, _title: String = "", _abstract: String = "", _keywords: List[String] = List.empty) = new WPSDataInput(_id, minoccurs, maxoccurs, _title, _abstract, _keywords)
}

class WPSDataInput(_id: String, val minoccurs: Int, val maxoccurs: Int, _title: String, _abstract: String, _keywords: List[String]) extends WPSElement with Serializable {
  val identifier = _id
  val title = _title
  val description = _abstract
  override val keywords = _keywords

  def Describe(syntax: ResponseSyntax.Value): xml.Elem = syntax match {
    case ResponseSyntax.WPS => <wps:Input minOccurs={minoccurs.toString} maxOccurs={maxoccurs.toString}>
      {getHeader(syntax)}
    </wps:Input>
    case ResponseSyntax.Generic => <input minOccurs={minoccurs.toString} maxOccurs={maxoccurs.toString}>
      {getHeader(syntax)}
    </input>
  }
}

object WPSProcessOutput {
  def apply(_id: String, mimeType: String = "text/xml", _title: String = "", _abstract: String = "", _keywords: List[String] = List.empty) = new WPSProcessOutput(_id, mimeType, _title, _abstract, _keywords)
}

class WPSProcessOutput(_id: String, val mimeType: String, _title: String, _abstract: String, _keywords: List[String]) extends WPSElement with Serializable {
  val identifier = _id
  val title = _title
  val description = _abstract
  override val keywords = _keywords

  def Describe(syntax: ResponseSyntax.Value): xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <wps:Output>
        {getHeader(syntax)}<ComplexOutput>
        <Default>
          <Format>
            <MimeType>
              {mimeType}
            </MimeType>
          </Format>
        </Default>
        <Supported>
          <Format>
            <MimeType>
              {mimeType}
            </MimeType>
          </Format>
        </Supported>
      </ComplexOutput>
      </wps:Output>
    case ResponseSyntax.Generic => <output>
      {getHeader(syntax)}
    </output>
  }
}

class WPSWorkflowProcess(val identifier: String, val description: String, val title: String, val inputs: List[WPSDataInput] = List.empty, val outputs: List[WPSProcessOutput] = List.empty) extends WPSProcess {

}

trait WPSProcess extends WPSElement {
  val outputs: List[WPSProcessOutput]

  def GetCapabilities(syntax: ResponseSyntax.Value): xml.Elem = <wps:Process wps:processVersion="1">
    {getHeader(syntax)}
  </wps:Process>

  def DescribeProcess(syntax: ResponseSyntax.Value): xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <wps:ProcessDescription wps:processVersion="2" storeSupported="true" statusSupported="false">
        {getHeader(syntax)}<wps:ProcessOutputs>
        {outputs.map(_.Describe(syntax))}
      </wps:ProcessOutputs>
      </wps:ProcessDescription>
    case ResponseSyntax.Generic =>
      <process>
        {getHeader(syntax)}<outputs>
        {outputs.map(_.Describe(syntax))}
      </outputs>
      </process>
  }

  def ExecuteHeader(syntax: ResponseSyntax.Value): xml.Elem = syntax match {
    case ResponseSyntax.WPS => <wps:Process wps:processVersion="1">getHeader(syntax)</wps:Process>
    case ResponseSyntax.Generic => <process>getHeader(syntax)</process>
  }

}

