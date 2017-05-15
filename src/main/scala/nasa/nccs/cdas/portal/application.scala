package nasa.nccs.cdas.portal
import java.nio.file.{Files, Path, Paths}

import nasa.nccs.cdas.engine.spark.CDSparkContext
import nasa.nccs.cdas.portal.CDASApplication.logger
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.cdas.portal.CDASPortal.ConnectionMode._
import nasa.nccs.cdas.utilities.appParameters
import nasa.nccs.utilities.Loggable

import scala.xml
import scala.io.Source

object CDASapp {
  def elem( array: Array[String], index: Int, default: String = "" ): String = if( array.length > index ) array(index) else default

  def getConfiguration( parameter_file_path: String ): Map[String, String] = {
    if( parameter_file_path.isEmpty ) { Map.empty[String, String]  }
    else if( Files.exists( Paths.get(parameter_file_path) ) ) {
      val params: Iterator[Array[String]] = for ( line <- Source.fromFile(parameter_file_path).getLines() ) yield { line.split('=') }
      Map( params.filter( _.length > 1 ).map( a => ( a.head.trim, a.last.trim ) ).toSeq: _* )
    }
    else { throw new Exception( "Can't find parameter file: " + parameter_file_path) }
  }
}

class CDASapp( mode: CDASPortal.ConnectionMode, request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends CDASPortal( mode, request_port, response_port ) {
  import CDASapp._
  val processManager = new ProcessManager( appConfiguration )
  val process = "cdas"
  val printer = new scala.xml.PrettyPrinter(200, 3)
  Runtime.getRuntime().addShutdownHook( new Thread() { override def run() { term() } } )


  override def postArray(header: String, data: Array[Byte]) = {

  }

  override def execUtility(utilSpec: Array[String]) = {

  }

  def getResult( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResult( process, resultSpec(0) )
    sendResponse( resultSpec(0), printer.format( result )  )
  }

  def getResultStatus( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResultStatus( process, resultSpec(0) )
    sendResponse( resultSpec(0), printer.format( result )  )
  }

  override def execute( taskSpec: Array[String] ) = {
    val process_name = elem(taskSpec,2)
    val datainputs = if( taskSpec.length > 3 ) wpsObjectParser.parseDataInputs( taskSpec(3) ) else Map.empty[String, Seq[Map[String, Any]]]
    val runargs = if( taskSpec.length > 4 ) wpsObjectParser.parseMap( taskSpec(4) ) else Map.empty[String, Any]
    val response = processManager.executeProcess( process, process_name, datainputs, runargs.mapValues(_.toString) )
    val responseType = runargs.getOrElse("result","xml")
    sendResponse( taskSpec(0), printer.format( response ) )
    if( responseType == "cdms" ) { sendDirectResponse( response ) }
  }

  def sendDirectResponse( response: xml.Elem ): Unit =  {
    val refs: xml.NodeSeq = response \\ "Output" \\ "Reference"
    val resultHref = refs.flatMap( _.attribute("href") ).find( _.nonEmpty ).map( _.text ) match {
      case Some( href ) => logger.info( "Do Nothing now- output written to disk")
      case None => logger.error( "Can't find result Id in direct response")
    }
  }

  override def getCapabilities(utilSpec: Array[String]) = {
    val result: xml.Elem = processManager.getCapabilities( process, elem(utilSpec,2) )
    sendResponse( utilSpec(0), printer.format( result ) )
  }

  override def describeProcess(procSpec: Array[String]) = {
    val result: xml.Elem = processManager.describeProcess( process, elem(procSpec,2) )
    sendResponse( procSpec(0), printer.format( result )  )
  }
}

object CDASApplication extends Loggable {
  def main(args: Array[String]) {
    import CDASapp._
    logger.info(s"Executing CDAS with args: ${args.mkString(",")}, nprocs: ${Runtime.getRuntime.availableProcessors()}")
    val connect_mode = elem(args, 0, "bind")
    val request_port = elem(args, 1, "0").toInt
    val response_port = elem(args, 2, "0").toInt
    val parameter_file = elem(args, 3, "")
    val appConfiguration = getConfiguration( parameter_file )
    val cmode = if (connect_mode.toLowerCase.startsWith("c")) CONNECT else BIND
    val app = new CDASapp(cmode, request_port, response_port, appConfiguration)
    app.run()
  }
}

object TestApplication extends Loggable {
  def main(args: Array[String]) {
    import CDASapp._
    logger.info(s"Executing Test with args: ${args.mkString(",")}}")
    val connect_mode = elem(args, 0, "bind")
    val request_port = elem(args, 1, "0").toInt
    val response_port = elem(args, 2, "0").toInt
    val parameter_file = elem(args, 3, "")
    val appConfiguration = getConfiguration( parameter_file )
    val cmode = if (connect_mode.toLowerCase.startsWith("c")) CONNECT else BIND

    val sc = CDSparkContext()
    val indices = sc.sparkContext.parallelize( Array( 1, 2, 3, 4, 5 ,6 ,7 ) )
    val timings = indices.map( i => System.currentTimeMillis() )
    val t0 = System.currentTimeMillis()
    val time_list = timings.collect().map( tval => (tval/1.0E6).toString ) mkString (", ")
    println( (t0/1.0E6).toString + ": " + time_list )
  }
}
