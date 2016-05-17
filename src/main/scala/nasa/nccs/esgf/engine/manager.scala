package nasa.nccs.esgf.engine
import nasa.nccs.esgf.process.TaskRequest
import org.slf4j.LoggerFactory

abstract class PluginExecutionManager {
  val logger = LoggerFactory.getLogger( this.getClass )

  def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem

  def describeProcess(identifier: String): xml.Elem

  def listProcesses(): xml.Elem
}

object demoExecutionManager extends PluginExecutionManager {

  override def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem = {
    val request = TaskRequest( process_name, datainputs )
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
    request.toXml
  }
  override def describeProcess(identifier: String): xml.Elem = {
    <process id={ identifier }></process>
  }
  override def listProcesses(): xml.Elem = {
    <processes></processes>
  }
}
