package nasa.nccs.wps

import nasa.nccs.cds2.utilities.appParameters

abstract class  WPSServer {
  def getProcesses: List[WPSProcess]

  def GetCapabilities: xml.Elem = {
    <wps:Capabilities service="WPS" version="1.0.0" xml:lang="en-CA" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsGetCapabilities_response.xsd" updateSequence="1">
      <ows:ServiceIdentification>
        <ows:Title>
          {appParameters("wps.server.title", "Climate Data Analytics Server (CDAS)")}
        </ows:Title>
        <ows:Abstract>
          {appParameters("wps.server.abstract", "High Performance Spark-based Climate Data Analytics delivered via the ESGF Compute Working Team WPS API")}
        </ows:Abstract>
        <ows:Keywords>
          <ows:Keyword>WPS</ows:Keyword>
          <ows:Keyword>ESGF-CWT</ows:Keyword>
          <ows:Keyword>CDAS</ows:Keyword>
          <ows:Keyword>Cliimate Data Analytics</ows:Keyword>
        </ows:Keywords>
        <ows:ServiceType>WPS</ows:ServiceType>
        <ows:ServiceTypeVersion>1.0.0</ows:ServiceTypeVersion>
        <ows:ServiceTypeVersion>0.4.0</ows:ServiceTypeVersion>
        <ows:Fees>NONE</ows:Fees>
        <ows:AccessConstraints>NONE</ows:AccessConstraints>
      </ows:ServiceIdentification>
      <ows:ServiceProvider>
        <ows:ProviderName>
          {appParameters("wps.server.provider.name", "NASA NCCS")}
        </ows:ProviderName>
        <ows:ProviderSite xlink:href={appParameters("wps.server.provider.url", "https://www.nccs.nasa.gov")}/>
        <ows:ServiceContact>
          <ows:IndividualName>
            {appParameters("wps.server.manager.name", "Thomas Maxwell")}
          </ows:IndividualName>
          <ows:PositionName>
            {appParameters("wps.server.manager.title", "Lead System Architect")}
          </ows:PositionName>
          <ows:ContactInfo>
            <ows:Phone>
              <ows:Voice>
                {appParameters("wps.server.manager.phone", "301-286-7810")}
              </ows:Voice>
            </ows:Phone>
            <ows:Address>
              <ows:ElectronicMailAddress>
                {appParameters("wps.server.manager.email", "thomas.maxwell@nasa.gov")}
              </ows:ElectronicMailAddress>
            </ows:Address>
          </ows:ContactInfo>
        </ows:ServiceContact>
      </ows:ServiceProvider>
      <ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities"/>
        <ows:Operation name="DescribeProcess"/>
        <ows:Operation name="Execute"/>
      </ows:OperationsMetadata>
      <wps:ProcessOfferings>
        {getProcesses.map(_.GetCapabilities)}
      </wps:ProcessOfferings>
    </wps:Capabilities>
  }

  def DescribeProcess: xml.Elem = {
    <wps:ProcessDescriptions xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsDescribeProcess_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA">
      {getProcesses.map(_.DescribeProcess)}
    </wps:ProcessDescriptions>
  }

}

class TestServer extends WPSServer {
  def getProcesses: List[WPSProcess] = List( TestProcess )
}

object ServerTest extends App {
  val printer = new scala.xml.PrettyPrinter(200, 3)
  val server = new TestServer()

  val response = server.DescribeProcess

  println( printer.format(response) )
}
