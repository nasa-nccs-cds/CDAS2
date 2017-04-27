import sbt._

object Version {
  val hadoop    = "2.6.0"
  val mockito   = "1.8.5"
  val scala     = "2.10.5"
  val scalaTest = "2.2.4"
}

object Library {
  val logback        = "ch.qos.logback"     %  "logback-core"   % "1.1.3"
  val scalatest      = "org.scalatest"     %% "scalatest"       % Version.scalaTest
  val scalaxml       = "org.scala-lang.modules" %% "scala-xml"  % "1.0.3"
  val commonsIO      = "commons-io"         % "commons-io"      % "2.5"
  val zeromq         = "org.zeromq"         % "jeromq"          % "0.3.5"
  val cdm            = "edu.ucar"           % "cdm"             % "4.6.8"
  val clcommon       = "edu.ucar"           % "clcommon"        % "4.6.8"
  val netcdf4        = "edu.ucar"           % "netcdf4"         % "4.6.8"
  val opendap        = "edu.ucar"           % "opendap"         % "4.6.8"
  val nd4s           = "org.nd4j"           % "nd4s_2.11"       % "0.4-rc3.8"
  val nd4j           =  "org.nd4j"          % "nd4j-x86"        % "0.4-rc3.8"
  val httpservices   = "edu.ucar"           %  "httpservices"   % "4.6.0"
  val udunits        = "edu.ucar"           %  "udunits"        % "4.6.0"
  val joda           = "joda-time"          % "joda-time"       % "2.8.1"
  val natty          = "com.joestelmach"    % "natty"           % "0.12"
  val py4j           = "net.sf.py4j"        % "py4j"            % "0.10.4"
  val geotools       = "org.geotools"      %  "gt-shapefile"    % "13.2"
  val scalactic      = "org.scalactic" %% "scalactic"          % "3.0.0"
  val concurrentlinkedhashmap = "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.4.2"
}

object Dependencies {
  import Library._

  val scala = Seq( natty, scalactic, commonsIO, scalatest, zeromq, logback )

  val cache = Seq( concurrentlinkedhashmap )

  val ndarray = Seq( nd4s, nd4j )

  val geo  = Seq( geotools )

  val netcdf = Seq( cdm, clcommon, netcdf4, opendap )
}

object cdas2Patch {
  def apply( filePath: sbt.File ) = {
    import scala.io.Source
    import java.io
    val old_lines = Source.fromFile(filePath).getLines.toList
    val new_lines = old_lines map { line =>
      val pos = line.indexOfSlice("app_classpath=")
      if (pos == -1) line else { line.slice(0, pos + 15) + "${CONDA_PREFIX}/lib:" + line.slice(pos + 15, line.length) }
    }
    val pw = new io.PrintWriter( new File(filePath.toString) )
    pw.write(new_lines.mkString("\n"))
    pw.close
    println( "Patched executable: " + filePath )
  }
}

//<groupId>com.googlecode.concurrentlinkedhashmap</groupId>
//  <artifactId>concurrentlinkedhashmap-lru</artifactId>
//  <version>1.4.2</version>










