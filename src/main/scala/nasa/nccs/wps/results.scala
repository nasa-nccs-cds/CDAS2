package nasa.nccs.wps

import nasa.nccs.cdapi.data.RDDPartition
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.cds2.utilities.appParameters
import nasa.nccs.esgf.process.{DataFragmentSpec, TargetGrid}
import nasa.nccs.utilities.Loggable

object WPSExecuteResponse {
  def merge(  serviceInstance: String, responses: List[WPSExecuteResponse] ): WPSExecuteResponse = new MergedWPSExecuteResponse( serviceInstance, responses )
}

trait WPSResponse {
  def toXml: xml.Elem
}

abstract class WPSExecuteResponse( val serviceInstance: String, val processes: List[WPSProcess] ) extends WPSResponse {
  val proxyAddress =  appParameters("wps.server.proxy.href","")
  def this( serviceInstance: String, process: WPSProcess ) = this( serviceInstance, List(process) )
  def getReference: xml.Elem

  def toXml: xml.Elem =
    <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" serviceInstance={serviceInstance} statusLocation={proxyAddress}>
      { processes.map( _.ExecuteHeader ) }
      <wps:Status> <wps:ProcessStarted> CDAS Process executing </wps:ProcessStarted> </wps:Status>
      <wps:ProcessOutputs> { getOutputs } </wps:ProcessOutputs>
    </wps:ExecuteResponse>

  def getOutputs: List[xml.Elem] = processes.flatMap( p => p.outputs.map( output => <wps:Output> { output.getHeader } { getReference } { getProcessOutputs( p.identifier, output.identifier) } </wps:Output> ) )
  def getProcessOutputs( process_id: String, output_id: String ): Iterable[xml.Elem]
  def getData( id: String, array: CDFloatArray, units: String, maxSize: Int = Int.MaxValue ): xml.Elem =
    <wps:Data id={id}> <wps:LiteralData uom={units} shape={array.getShape.mkString(",")}>{ array.mkBoundedDataString(",",maxSize) }</wps:LiteralData> </wps:Data>
}

abstract class WPSReferenceExecuteResponse( serviceInstance: String, val process: WPSProcess, val optResultId: Option[String] )  extends WPSExecuteResponse( serviceInstance, process )  {
  val statusHref: String = optResultId match { case Some( rid ) => proxyAddress + s"/wps/status?id=$rid"; case None => "" }
  def getReference: xml.Elem = <wps:Reference encoding="UTF-8" mimeType="text/xml" href={statusHref}/>
}

class MergedWPSExecuteResponse( serviceInstance: String, responses: List[WPSExecuteResponse] ) extends WPSExecuteResponse( serviceInstance, responses.flatMap(_.processes) ) with Loggable {
  val process_ids: List[String] = responses.flatMap( response => response.processes.map( process => process.identifier ) )
  def getReference: xml.Elem = responses.head.getReference
  if( process_ids.distinct.size != process_ids.size ) { logger.warn( "Error, non unique process IDs in process list: " + processes.mkString(", ") ) }
  val responseMap: Map[String,WPSExecuteResponse] = Map( responses.flatMap( response => response.processes.map( process => ( process.identifier -> response ) ) ): _* )
  def getProcessOutputs( process_id: String, response_id: String ): Iterable[xml.Elem] = responseMap.get( process_id ) match {
    case Some( response ) => response.getProcessOutputs(process_id, response_id);
    case None => throw new Exception( "Unrecognized process id: " + process_id )
  }
}

class RDDExecutionResult( serviceInstance: String, process: WPSProcess, id: String, val result: RDDPartition,  optResultId: Option[String] = None ) extends WPSReferenceExecuteResponse( serviceInstance, process, optResultId )  with Loggable {
  def getProcessOutputs( process_id: String, output_id: String  ): Iterable[xml.Elem] = {
    result.elements map { case (id, array) => getData( id, array.toCDFloatArray, array.metadata.getOrElse("units","") ) }
  }
}

class ExecutionErrorReport( serviceInstance: String, process: WPSProcess, id: String, val err: Throwable ) extends WPSReferenceExecuteResponse( serviceInstance, process, None )  with Loggable {
  print_error
  override def toXml = {
    <ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
      { getReport }
    </ows:ExceptionReport>
  }
  def getReport: Iterable[xml.Elem] =  List(  <ows:Exception exceptionCode={err.getClass.getName}> <ows:ExceptionText>{err.getMessage}</ows:ExceptionText> </ows:Exception> )
  def print_error = {
    val err1 = if (err.getCause == null) err else err.getCause
    logger.error("\n\n-------------------------------------------\n" + err1.toString + "\n")
    logger.error(  err1.getStackTrace.mkString("\n")  )
    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
    logger.error( "\n-------------------------------------------\n\n")
  }
  def getProcessOutputs( process_id: String, response_id: String ): Iterable[xml.Elem] = Iterable.empty[xml.Elem]
}


abstract class WPSEventReport extends WPSResponse {
  def toXml: xml.Elem =  <EventReports>  { getReport } </EventReports>
  def getReport: Iterable[xml.Elem]
}

class UtilityExecutionResult( id: String, val report: xml.Elem )  extends WPSEventReport with Loggable {
  def getReport: Iterable[xml.Elem] =  List( <UtilityReport utilityId={id}>  { report } </UtilityReport> )
}

class WPSExceptionReport( val err: Throwable ) extends WPSEventReport with Loggable {
  print_error
  override def toXml = {
    <ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
      { getReport }
    </ows:ExceptionReport>
  }
  def getReport: Iterable[xml.Elem] =  List(  <ows:Exception exceptionCode={err.getClass.getName}> <ows:ExceptionText>{err.getMessage}</ows:ExceptionText> </ows:Exception> )
  def print_error = {
    val err1 = if (err.getCause == null) err else err.getCause
    logger.error("\n\n-------------------------------------------\n" + err1.toString + "\n")
    logger.error(  err1.getStackTrace.mkString("\n")  )
    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
    logger.error( "\n-------------------------------------------\n\n")
  }
}

class AsyncExecutionResult( serviceInstance: String, process: WPSProcess, optResultId: Option[String] ) extends WPSReferenceExecuteResponse( serviceInstance, process, optResultId )  {
  def getProcessOutputs( process_id: String, output_id: String ): Iterable[xml.Elem] = List()
}

class WPSMergedEventReport( val reports: List[WPSEventReport] ) extends WPSEventReport {
  def getReport: Iterable[xml.Elem] = reports.flatMap( _.getReport )
}

class BlockingExecutionResult( serviceInstance: String, process: WPSProcess, id: String, val intputSpecs: List[DataFragmentSpec], val gridSpec: TargetGrid, val result_tensor: CDFloatArray,
                               optResultId: Option[String] = None ) extends WPSReferenceExecuteResponse( serviceInstance, process, optResultId )  with Loggable {
  //  def toXml_old = {
  //    val idToks = id.split('-')
  //    logger.info( "BlockingExecutionResult-> result_tensor(" + id + "): \n" + result_tensor.toString )
  //    val inputs = intputSpecs.map( _.toXml )
  //    val grid = gridSpec.toXml
  //    val results = result_tensor.mkDataString(",")
  //    <result id={id} op={idToks.head} rid={resultId.getOrElse("")}> { inputs } { grid } <data undefined={result_tensor.getInvalid.toString}> {results}  </data>  </result>
  //  }
  def getProcessOutputs( process_id: String, output_id: String  ): Iterable[xml.Elem] = List( getData( output_id, result_tensor, intputSpecs.head.units, 250 ) )
}

