package nasa.nccs.wps

import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.cdapi.data.{RDDRecord}
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cdas.utilities.appParameters
import nasa.nccs.esgf.process.{DataFragmentSpec, TargetGrid}
import nasa.nccs.utilities.Loggable
import scala.xml

object WPSProcessExecuteResponse {
  def merge(  serviceInstance: String, responses: List[WPSProcessExecuteResponse] ): WPSProcessExecuteResponse = new MergedWPSExecuteResponse( serviceInstance, responses )
}

object ResponseSyntax extends Enumeration {
  val WPS, Generic = Value
}

trait WPSResponse extends {
  val proxyAddress =  appParameters("wps.server.proxy.href","")
  val syntax =  appParameters("wps.response.syntax","") match {
    case "generic" => ResponseSyntax.Generic
    case _ => ResponseSyntax.WPS
  }
  def toXml: xml.Elem
}

class WPSExecuteStatus( val serviceInstance: String,  val statusMessage: String, val resId: String  ) extends WPSResponse {
  val resultHref: String = proxyAddress + s"/wps/file?id=$resId"

  def toXml: xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA">
        <wps:Status>
          <wps:ProcessStarted>{statusMessage}</wps:ProcessStarted>
        </wps:Status>
        <wps:Reference encoding="UTF-8" mimeType="application/x-netcdf" href={resultHref}/>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
      <response serviceInstance={serviceInstance} statusLocation={proxyAddress} status={statusMessage} href={resultHref}/>
  }
}


class WPSExecuteResult( val serviceInstance: String, val tvar: RDDTransientVariable) extends WPSResponse {

  def toXml: xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA">
        <wps:Status>
          <wps:ProcessSucceeded>CDAS Process successfully calculated</wps:ProcessSucceeded>
        </wps:Status>
        <wps:ProcessOutputs>
          {tvar.result.elements.map { case (id, result) =>
          <wps:Output>
            <wps:Data id={id}>
              <wps:LiteralData uom={result.metadata.getOrElse("units", "")} shape={result.shape.mkString(",")}>
                {result.toCDFloatArray.mkDataString(" ", " ", " ")}
              </wps:LiteralData>
            </wps:Data>
          </wps:Output>
        }}
        </wps:ProcessOutputs>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
      <response  serviceInstance={serviceInstance} statusLocation={proxyAddress} status="Success">
        <outputs>
          {tvar.result.elements.map { case (id, result) =>
          <output id={id} uom={result.metadata.getOrElse("units", "")} shape={result.shape.mkString(",")} >
                {result.toCDFloatArray.mkDataString(" ", " ", " ")}
          </output>
        }}
        </outputs>
      </response>
  }
}

abstract class WPSExecuteResponse( val serviceInstance: String ) extends WPSResponse {

  def getData( syntax: ResponseSyntax.Value, id: String, array: CDFloatArray, units: String, maxSize: Int = Int.MaxValue ): xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <wps:Data id={id}>
        <wps:LiteralData uom={units} shape={array.getShape.mkString(",")}>  {array.mkBoundedDataString(",", maxSize)}  </wps:LiteralData>
      </wps:Data>
    case ResponseSyntax.Generic =>
      <data id={id} uom={units} shape={array.getShape.mkString(",")}>  {array.mkBoundedDataString(",", maxSize)}  </data>
  }

  def getDataRef( syntax: ResponseSyntax.Value, id: String, resultId: String, optFileRef: Option[String] ): xml.Elem = syntax match {
    case ResponseSyntax.WPS => optFileRef match {
      case Some( fileRef ) => <wps:Data id={id} href={"result://"+resultId} file={fileRef}> </wps:Data>
      case None =>            <wps:Data id={id} href={"result://"+resultId}> </wps:Data>
    }
    case ResponseSyntax.Generic => optFileRef match {
      case Some( fileRef ) => <data id={id} href={"result://"+resultId} file={fileRef}> </data>
      case None =>            <data id={id} href={"result://"+resultId}> </data>
    }
  }
}

abstract class WPSProcessExecuteResponse( serviceInstance: String, val processes: List[WPSProcess] ) extends WPSExecuteResponse(serviceInstance) {
  def this( serviceInstance: String, process: WPSProcess ) = this( serviceInstance, List(process) )
  def getReference: xml.Elem
  def getFileReference: xml.Elem
  def getResultReference: xml.Elem

  def toXml: xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" serviceInstance={serviceInstance} statusLocation={proxyAddress}>
        {processes.map(_.ExecuteHeader)}<wps:Status>
        <wps:ProcessStarted>CDAS Process executing</wps:ProcessStarted>
      </wps:Status>
        <wps:ProcessOutputs>
          {getOutputs}
        </wps:ProcessOutputs>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
      <response>  {getOutputs} </response>
  }

  def getOutputs: List[xml.Elem] = syntax match {
    case ResponseSyntax.Generic =>
      processes.flatMap(p => p.outputs.map(output => <outputs> {getProcessOutputs(syntax, p.identifier, output.identifier)} </outputs>))
    case ResponseSyntax.WPS =>
      processes.flatMap(p => p.outputs.map(output => <wps:Output>
        {output.getHeader}{getProcessOutputs(syntax,p.identifier, output.identifier)}
      </wps:Output>))
  }

  def getProcessOutputs(syntax: ResponseSyntax.Value, process_id: String, output_id: String ): Iterable[xml.Elem]

}

abstract class WPSReferenceExecuteResponse( serviceInstance: String, val process: WPSProcess, val resultId: String )  extends WPSProcessExecuteResponse( serviceInstance, process )  {
  val statusHref: String = proxyAddress + s"/wps/status?id=$resultId"
  val fileHref: String = proxyAddress + s"/wps/file?id=$resultId"
  val resultHref: String = proxyAddress + s"/wps/result?id=$resultId"
  def getReference: xml.Elem = syntax match {
    case ResponseSyntax.WPS => <wps:Reference id="status" encoding="UTF-8" mimeType="text/xml" href={statusHref}/>
    case ResponseSyntax.Generic => <reference id="status" href={statusHref}/>
  }
  def getFileReference: xml.Elem = syntax match {
    case ResponseSyntax.WPS => <wps:Reference id="file" encoding="UTF-8" mimeType="text/xml" href={fileHref}/>
    case ResponseSyntax.Generic =>   <reference id="file" href={fileHref}/>
  }
  def getResultReference: xml.Elem = syntax match {
    case ResponseSyntax.WPS =>  <wps:Reference id="result" encoding="UTF-8" mimeType="text/xml" href={resultHref}/>
    case ResponseSyntax.Generic => <reference id="result" href={resultHref}/>
  }
  def getResultId: String = resultId
}

abstract class WPSDirectExecuteResponse( serviceInstance: String, val process: WPSProcess, val resultId: String, resultFileOpt: Option[String] )  extends WPSProcessExecuteResponse( serviceInstance, process )  {
  val statusHref: String = ""
  val fileHref: String = "file://" + resultFileOpt.getOrElse("")
  val resultHref: String = s"result://$resultId"
  def getOutputTag = syntax match {
    case ResponseSyntax.WPS => "Output"
    case ResponseSyntax.Generic => "output"
  }
  def getReference: xml.Elem = syntax match {
    case ResponseSyntax.WPS => <wps:Reference id="status" href={statusHref}/>
    case ResponseSyntax.Generic => <reference id="status" href={statusHref}/>
  }
  def getFileReference: xml.Elem = syntax match {
    case ResponseSyntax.WPS => <wps:Reference id="file" href={fileHref}/>
    case ResponseSyntax.Generic =>   <reference id="file" href={fileHref}/>
  }
  def getResultReference: xml.Elem = syntax match {
    case ResponseSyntax.WPS =>  <wps:Reference id="result" href={resultHref}/>
    case ResponseSyntax.Generic => <reference id="result" href={resultHref}/>
  }
  def getResultId: String = resultId
}

class MergedWPSExecuteResponse( serviceInstance: String, responses: List[WPSProcessExecuteResponse] ) extends WPSProcessExecuteResponse( serviceInstance, responses.flatMap(_.processes) ) with Loggable {
  val process_ids: List[String] = responses.flatMap( response => response.processes.map( process => process.identifier ) )
  def getReference: xml.Elem = responses.head.getReference
  def getFileReference: xml.Elem = responses.head.getFileReference
  def getResultReference: xml.Elem = responses.head.getResultReference
  if( process_ids.distinct.size != process_ids.size ) { logger.warn( "Error, non unique process IDs in process list: " + processes.mkString(", ") ) }
  val responseMap: Map[String,WPSProcessExecuteResponse] = Map( responses.flatMap( response => response.processes.map( process => ( process.identifier -> response ) ) ): _* )
  def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, response_id: String ): Iterable[xml.Elem] = responseMap.get( process_id ) match {
    case Some( response ) =>
      response.getProcessOutputs( syntax, process_id, response_id );
    case None => throw new Exception( "Unrecognized process id: " + process_id )
  }
}

class RDDExecutionResult(serviceInstance: String, process: WPSProcess, id: String, val result: RDDRecord, resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, process, resultId )  with Loggable {
  def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = {
    result.elements map { case (id, array) => getData( syntax, id, array.toCDFloatArray, array.metadata.getOrElse("units","") ) }
  }
}

class RefExecutionResult(serviceInstance: String, process: WPSProcess, id: String, resultId: String, resultFileOpt: Option[String] ) extends WPSDirectExecuteResponse( serviceInstance, process, resultId, resultFileOpt )  with Loggable {
  def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = {
    Seq( getDataRef( syntax, id, resultId, resultFileOpt ) )
  }
}


class ExecutionErrorReport( serviceInstance: String, process: WPSProcess, id: String, val err: Throwable ) extends WPSReferenceExecuteResponse( serviceInstance, process, "" )  with Loggable {
  print_error
  override def toXml: xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
        {getReport} </ows:ExceptionReport>
    case ResponseSyntax.Generic => <response> <exceptions> {getReport} </exceptions> </response>
  }
  def getReport: Iterable[xml.Elem] =  syntax match {
    case ResponseSyntax.WPS =>
      List(<ows:Exception exceptionCode={err.getClass.getName}> <ows:ExceptionText>  {err.getMessage} </ows:ExceptionText> </ows:Exception>)
    case ResponseSyntax.Generic =>
      List(<exception name={err.getClass.getName}> {err.getMessage} </exception>)
  }
  def print_error = {
    val err1 = if (err.getCause == null) err else err.getCause
    logger.error("\n\n-------------------------------------------\n" + err1.toString + "\n")
    logger.error(  err1.getStackTrace.mkString("\n")  )
    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
    logger.error( "\n-------------------------------------------\n\n")
  }
  def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, response_id: String ): Iterable[xml.Elem] = Iterable.empty[xml.Elem]
}


abstract class WPSEventReport extends WPSResponse {
  def toXml: xml.Elem =  <response> <EventReports>  { getReport } </EventReports> </response>
  def getReport: Iterable[xml.Elem]
}

class UtilityExecutionResult( id: String, val report: xml.Elem )  extends WPSEventReport with Loggable {
  def getReport: Iterable[xml.Elem] =  List( <UtilityReport utilityId={id}>  { report } </UtilityReport> )
}

class WPSExceptionReport( val err: Throwable, serviceInstance: String = "WPS" ) extends WPSExecuteResponse(serviceInstance) with Loggable {
  print_error
  def toXml: xml.Elem = syntax match {
    case ResponseSyntax.WPS =>
      <ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
        {getReport} </ows:ExceptionReport>
    case ResponseSyntax.Generic => <response> <exceptions> {getReport} </exceptions> </response>
      }
  def getReport: Iterable[xml.Elem] =  syntax match {
    case ResponseSyntax.WPS =>
      List(<ows:Exception exceptionCode={err.getClass.getName}> <ows:ExceptionText>  {err.getMessage} </ows:ExceptionText> </ows:Exception>)
    case ResponseSyntax.Generic =>
      List(<exception name={err.getClass.getName}> {err.getMessage} </exception>)
  }
  def print_error = {
    val err1 = if (err.getCause == null) err else err.getCause
    logger.error("\n\n-------------------------------------------\n" + err1.toString + "\n")
    logger.error(  err1.getStackTrace.mkString("\n")  )
    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
    logger.error( "\n-------------------------------------------\n\n")
  }
  def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, response_id: String ): Iterable[xml.Elem] = Iterable.empty[xml.Elem]
}


class AsyncExecutionResult( serviceInstance: String, process: WPSProcess, resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, process, resultId )  {
  def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, output_id: String ): Iterable[xml.Elem] = List()
}

class WPSMergedEventReport( val reports: List[WPSEventReport] ) extends WPSEventReport {
  def getReport: Iterable[xml.Elem] = reports.flatMap( _.getReport )
}

class WPSMergedExceptionReport( val exceptions: List[WPSExceptionReport] ) extends WPSEventReport {
  def getReport: Iterable[xml.Elem] = exceptions.flatMap( _.getReport )
}

class BlockingExecutionResult( serviceInstance: String, process: WPSProcess, id: String, val intputSpecs: List[DataFragmentSpec], val gridSpec: TargetGrid, val result_tensor: CDFloatArray,
                               resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, process, resultId )  with Loggable {
  //  def toXml_old = {
  //    val idToks = id.split('-')
  //    logger.info( "BlockingExecutionResult-> result_tensor(" + id + "): \n" + result_tensor.toString )
  //    val inputs = intputSpecs.map( _.toXml )
  //    val grid = gridSpec.toXml
  //    val results = result_tensor.mkDataString(",")
  //    <result id={id} op={idToks.head} rid={resultId.getOrElse("")}> { inputs } { grid } <data undefined={result_tensor.getInvalid.toString}> {results}  </data>  </result>
  //  }
  def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = List( getData( syntax, output_id, result_tensor, intputSpecs.head.units, 250 ) )
}

