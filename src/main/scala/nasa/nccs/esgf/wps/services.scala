package nasa.nccs.esgf.wps

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.ExecutionException

import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.wps.{BlockingExecutionResult, WPSExceptionReport, WPSResponse}
import nasa.nccs.utilities.{Loggable, cdsutils}

import scala.xml

trait ServiceProvider extends Loggable {

  def executeProcess(identifier: String, parsed_data_inputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem

  //  def listProcesses(): xml.Elem

  def describeWPSProcess( identifier: String ): xml.Elem

  def getWPSCapabilities( identifier: String ): xml.Elem

  def getCause( e: Throwable ): Throwable = e match {
    case err: ExecutionException => err.getCause; case x => e
  }

  def getResultFilePath( resultId: String ): Option[String]
  def getResultVariable( resultId: String ): Option[RDDTransientVariable]
  def getResult( resultId: String ): xml.Node
  def getResultStatus( resultId: String ): xml.Node

  def fatal( e: Throwable ): WPSExceptionReport = {
    val err = getCause( e )
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    new WPSExceptionReport(err)
  }

  def shutdown()

}

object cds2ServiceProvider extends ServiceProvider {
  import nasa.nccs.cdas.engine.CDS2ExecutionManager
  import nasa.nccs.esgf.process.TaskRequest

  val cds2ExecutionManager = try { new CDS2ExecutionManager() } catch {
    case err: Throwable =>
      logger.error( "  *** ERROR initializing CDS2ExecutionManager: " + err.toString );
      err.printStackTrace();
      shutdown()
      throw err
    }

  def shutdown() = { CDS2ExecutionManager.shutdown(); }

  def datainputs2Str( datainputs: Map[String, Seq[Map[String, Any]]] ): String = {
    datainputs.map { case ( key:String, value:Seq[Map[String, Any]] ) =>
      key  + ": " + value.map( _.map { case (k1:String, v1:Any) => k1 + "=" + v1.toString  }.mkString(", ") ).mkString("{ ",", "," }")  }.mkString("{ ",", "," }")
  }

  override def executeProcess(process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem = {
    try {
      logger.info( " @@cds2ServiceProvider: exec process: " + process_name )
      cdsutils.time(logger, "\n\n-->> Process %s, datainputs: %s \n\n".format(process_name, datainputs2Str(datainputs))) {
        if (runargs.getOrElse("async", "false").toBoolean) {
          val result = cds2ExecutionManager.asyncExecute(TaskRequest(process_name, datainputs), runargs)
          result.toXml
        } else {
          val result = cds2ExecutionManager.blockingExecute(TaskRequest(process_name, datainputs), runargs)
          result.toXml
        }
      }
    } catch {
      case e: Exception => fatal(e).toXml
    }
  }
  def describeWPSProcess(process_name: String): xml.Elem = {
    try {
      cds2ExecutionManager.describeWPSProcess( process_name )

    } catch { case e: Exception => fatal(e).toXml }
  }
  def getWPSCapabilities(identifier: String): xml.Elem = {
    try {
      cds2ExecutionManager.getWPSCapabilities( if(identifier == null) "" else identifier )

    } catch { case e: Exception => fatal(e).toXml }
  }
  override def getResultFilePath( resultId: String ): Option[String] = cds2ExecutionManager.getResultFilePath( resultId )
  override def getResult( resultId: String ): xml.Node = cds2ExecutionManager.getResult( resultId )
  override def getResultVariable( resultId: String ): Option[RDDTransientVariable] = cds2ExecutionManager.getResultVariable( resultId )
  override def getResultStatus( resultId: String ): xml.Node = cds2ExecutionManager.getResultStatus( resultId )

}