import sbt._

object Version {
  val hadoop    = "2.6.0"
  val logback   = "1.1.3"
  val mockito   = "1.10.19"
  val scala     = "2.11.7"
  val scalaTest = "2.2.4"
  val slf4j     = "1.7.6"
  val spark     = "1.4.1"
}

object Library {
  val hadoopClient   = "org.apache.hadoop" %  "hadoop-client"   % Version.hadoop
  val logbackClassic = "ch.qos.logback"    %  "logback-classic" % Version.logback
  val mockitoAll     = "org.mockito"       %  "mockito-all"     % Version.mockito
  val scalaTest      = "org.scalatest"     %% "scalatest"       % Version.scalaTest
  val slf4jApi       = "org.slf4j"         %  "slf4j-api"       % Version.slf4j
  val sparkSQL       = "org.apache.spark"  %% "spark-sql"       % Version.spark
  val sparkCore      = "org.apache.spark"  %% "spark-core"      % "1.6.0"
  val scalaxml       = "org.scala-lang.modules" %% "scala-xml"  % "1.0.3"
  val scalaparser    = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"

  val cdm            = "edu.ucar"           % "cdm"             % "4.4.0"
  val clcommon       = "edu.ucar"           % "clcommon"        % "4.4.0"
  val netcdf4        = "edu.ucar"           % "netcdf4"         % "4.4.0"
  val netcdfAll      = "edu.ucar"           % "netcdfAll"       % "4.6.4"
  val nd4s           = "org.nd4j"           % "nd4s_2.11"       % "0.4-rc3.8"
  val nd4j           =  "org.nd4j"          % "nd4j-x86"        % "0.4-rc3.8"
  val opendap        = "edu.ucar"           % "opendap"         % "2.2.2"
  val httpservices   = "edu.ucar"           %  "httpservices"   % "4.6.0"
  val udunits        = "edu.ucar"           %  "udunits"        % "4.6.0"
  val joda           = "joda-time"          % "joda-time"       % "2.8.1"
  val natty          = "com.joestelmach"    % "natty"           % "0.11"
  val guava          = "com.google.guava"   % "guava"           % "18.0"
  val geotools       = "org.geotools"      %  "gt-shapefile"    % "15-SNAPSHOT"
  val breeze         = "org.scalanlp"      %% "breeze"          % "0.12"
  val kernelmod      = "nasa.nccs"         %% "kermodbase"      % "1.0-SNAPSHOT"
  val cdapi          = "nasa.nccs"         %% "cdapi"           % "1.0-SNAPSHOT"
  val sprayCache     = "io.spray"       % "spray-caching_2.11" % "1.3.3"
  val sprayUtil      = "io.spray"       % "spray-util_2.11"    % "1.3.3"
  val scalactic      = "org.scalactic" %% "scalactic" % "2.2.6"
  val scalatest      = "org.scalatest" %% "scalatest" % "2.2.6" % "test"
}

object Dependencies {
  import Library._

  val sparkAkkaHadoop = Seq(
    sparkSQL,
    hadoopClient,
    logbackClassic % "test",
    scalaTest      % "test",
    mockitoAll     % "test"
  )
  val scala = Seq( logbackClassic, joda, natty, scalactic, scalatest )

  val spark = Seq( sparkCore )

  val cache = Seq( sprayCache, sprayUtil )

  val ndarray = Seq( nd4s, nd4j )

  val kernels = Seq( kernelmod, cdapi )
}

//<groupId>com.googlecode.concurrentlinkedhashmap</groupId>
//  <artifactId>concurrentlinkedhashmap-lru</artifactId>
//  <version>1.4.2</version>










