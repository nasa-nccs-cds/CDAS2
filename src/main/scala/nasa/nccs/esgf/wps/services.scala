package nasa.nccs.esgf.wps

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.ExecutionException

import nasa.nccs.cdapi.kernels.{BlockingExecutionResult, ErrorExecutionResult, ExecutionResults, XmlExecutionResult}
import nasa.nccs.cds2.engine.futures.CDFuturesExecutionManager
import nasa.nccs.esgf.engine.demoExecutionManager
import nasa.nccs.utilities.cdsutils
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

abstract class ServiceProvider( val serverConfiguration: Map[String,String] ) {
  val logger = LoggerFactory.getLogger(this.getClass)

  def executeProcess(identifier: String, parsed_data_inputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem

  //  def listProcesses(): xml.Elem

  def describeProcess( identifier: String ): xml.Elem

  def getCapabilities( identifier: String ): xml.Elem

  def getCause( e: Throwable ): Throwable = e match {
    case err: ExecutionException => err.getCause; case x => e
  }

  def getResultFilePath( resultId: String ): Option[String]

  def fatal( e: Throwable ): xml.Elem = {
    val err = getCause( e )
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    <error id="Execution Error"> { err.getMessage } </error>
  }

}

class esgfServiceProvider( serverConfiguration: Map[String,String] ) extends ServiceProvider(serverConfiguration) {
  import nasa.nccs.esgf.engine.demoExecutionManager

  override def executeProcess(process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem = {
    try { demoExecutionManager.execute(process_name, datainputs, runargs) } catch { case e: Exception => <error id="Execution Error"> {e.getMessage} </error> }
  }
  override def describeProcess(process_name: String): xml.Elem = {
    try {  demoExecutionManager.describeProcess(process_name) } catch {  case e: Exception => <error id="Execution Error"> {e.getMessage} </error> }
  }
  override def getCapabilities(identifier: String): xml.Elem = {
    try {  demoExecutionManager.listProcesses() } catch {  case e: Exception => <error id="Execution Error"> {e.getMessage} </error> }
  }
  override def getResultFilePath( resultId: String ): Option[String] = None
}

class cds2ServiceProvider( serverConfiguration: Map[String,String] ) extends ServiceProvider(serverConfiguration) {
  import nasa.nccs.cds2.engine.CDS2ExecutionManager
  import nasa.nccs.esgf.process.TaskRequest

  val cds2ExecutionManager = new CDFuturesExecutionManager( serverConfiguration )

  def datainputs2Str( datainputs: Map[String, Seq[Map[String, Any]]] ): String = {
    datainputs.map { case ( key:String, value:Seq[Map[String, Any]] ) =>
      key  + ": " + value.map( _.map { case (k1:String, v1:Any) => k1 + "=" + v1.toString  }.mkString(", ") ).mkString("{ ",", "," }")  }.mkString("{ ",", "," }")
  }

  override def executeProcess(process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem = {
    try {
      cdsutils.time( logger, "\n\n-->> Process %s, datainputs: %s \n\n".format( process_name, datainputs2Str(datainputs) ) ) {
        if( runargs.getOrElse("async","false").toBoolean ) {
          cds2ExecutionManager.asyncExecute(TaskRequest(process_name, datainputs), runargs) match {
            case ( mdata: Map[String,String], futureResult: Future[ExecutionResults] ) =>
              mdata.get("results") match {
                case Some(fragments) => <result fragments={fragments}/>
                case None => <result url={"http://server:port/wps/results?id=%s".format(mdata.getOrElse("job",""))} />
              }
            case x =>  <error id="Execution Error"> {"Malformed response from cds2ExecutionManager" } </error>
          }
        }
        else  {
          cds2ExecutionManager.blockingExecute(TaskRequest(process_name, datainputs), runargs).toXml
        }
      }
    } catch { case e: Exception => fatal(e) }
  }
  override def describeProcess(process_name: String): xml.Elem = {
    try {
      cds2ExecutionManager.describeProcess( process_name )

    } catch { case e: Exception => fatal(e) }
  }
  override def getCapabilities(identifier: String): xml.Elem = {
    try {
      cds2ExecutionManager.getCapabilities( if(identifier == null) "" else identifier )

    } catch { case e: Exception => fatal(e) }
  }
  override def getResultFilePath( resultId: String ): Option[String] = cds2ExecutionManager.getResultFilePath( resultId )
}


object resourceTest extends App {
  import nasa.nccs.cds2.engine.CDS2ExecutionManager
  val serverConfiguration: Map[String,String] = Map()

  val cds2ExecutionManager = new CDFuturesExecutionManager( serverConfiguration )

  val resourcePath = cds2ExecutionManager.getResourcePath("/collections.xml")
  println( resourcePath )
}
