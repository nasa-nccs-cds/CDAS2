import java.io.PrintWriter
import java.nio.file.Files.copy
import java.nio.file.Paths.get
import java.nio.file.StandardCopyOption

import sbt.{SettingKey, _}


val kernelPackages = settingKey[ Seq[String] ]("A list of user-defined Kernel packages")

name := "CDAS2"
version := "1.2.2-SNAPSHOT"
scalaVersion := "2.10.5"
organization := "nasa.nccs"

lazy val root = project in file(".")
val sbtcp = taskKey[Unit]("sbt-classpath")
val upscr = taskKey[Unit]("update-cdas-scripts")

resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
resolvers += "Java.net repository" at "http://download.java.net/maven/2"
resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"
resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Boundless Maven Repository" at "http://repo.boundlessgeo.com/main"
resolvers += "spray repo" at "http://repo.spray.io"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "Geotoolkit" at "http://maven.geotoolkit.org/"
resolvers += "Maven Central" at "http://central.maven.org/maven2/"

enablePlugins(JavaAppPackaging)

mainClass in (Compile, run) := Some("nasa.nccs.cdas.portal.CDASApplication")
mainClass in (Compile, packageBin) := Some("nasa.nccs.cdas.portal.CDASApplication")

libraryDependencies ++= ( Dependencies.cache ++ Dependencies.geo ++ Dependencies.netcdf ++ Dependencies.socket ++ Dependencies.utils ++ Dependencies.test )

libraryDependencies ++= {
  sys.env.get("YARN_CONF_DIR") match {
    case Some(yarn_config) => Seq.empty
    case None => Dependencies.spark ++ Dependencies.scala                     // ++ Dependencies.xml     : For 2.11 or later!
  }
}

dependencyOverrides ++= Set( "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4" )

sbtcp := {
  val files: Seq[String] = (fullClasspath in Compile).value.files.map(x => x.getAbsolutePath)
  val sbtClasspath : String = files.mkString(":")
  println("Set SBT classpath to 'sbt-classpath' environment variable")
  System.setProperty("sbt-classpath", sbtClasspath)
}

compile  <<= (compile in Compile).dependsOn(sbtcp)

fork := true

logBuffered in Test := false

javaOptions in run ++= Seq( "-Xmx8000M", "-Xms512M", "-Xss1M", "-XX:+CMSClassUnloadingEnabled", "-XX:+UseConcMarkSweepGC")
javaOptions in test ++= Seq( "-Xmx8000M", "-Xms512M", "-Xss1M", "-XX:+CMSClassUnloadingEnabled", "-XX:+UseConcMarkSweepGC", "-XX:+PrintFlagsFinal")
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

import java.util.Properties
lazy val cdasPropertiesFile = settingKey[File]("The cdas properties file")
lazy val cdasDefaultPropertiesFile = settingKey[File]("The cdas defaultproperties file")
lazy val cdasPythonRunScript = settingKey[File]("The cdas python worker startup script")
lazy val cdasDefaultPythonRunScript = settingKey[File]("The default cdas python worker startup script")
lazy val cdasStandaloneRunScript = settingKey[File]("The cdas spark-cluster startup script")
lazy val cdasDefaultStandaloneRunScript = settingKey[File]("The default cdas spark-cluster startup script")
lazy val cdasPythonShutdownScript = settingKey[File]("The cdas python worker shutdown script")
lazy val cdasDefaultPythonShutdownScript = settingKey[File]("The default cdas python worker shutdown script")
lazy val cdasSetupScript = settingKey[File]("The cdas setup runtime script")
lazy val cdasDefaultSetupScript = settingKey[File]("The default cdas setup runtime script")
lazy val cdasLocalCollectionsFile = settingKey[File]("The cdas local Collections file")
lazy val cdas_cache_dir = settingKey[File]("The CDAS cache directory.")
lazy val cdas_conf_dir = settingKey[File]("The CDAS conf directory.")
lazy val cdas_sbin_dir = settingKey[File]("The CDAS sbin directory.")
lazy val cdas_logs_dir = settingKey[File]("The CDAS logs directory.")
lazy val conda_lib_dir = settingKey[File]("The Conda lib directory.")
val cdasProperties = settingKey[Properties]("The cdas properties map")

cdas_conf_dir := baseDirectory.value / "src" / "universal" / "conf"
cdas_sbin_dir := getCDASbinDir
cdas_logs_dir := getCDASlogsDir
conda_lib_dir := getCondaLibDir

unmanagedJars in Compile ++= {
  sys.env.get("CDAS_UNMANAGED_JARS") match {
    case Some(jars_dir) =>
      val customJars: PathFinder =  file(jars_dir) ** (("*.jar" -- "*netcdf*") -- "*concurrentlinkedhashmap*")
      val classpath_file = cdas_cache_dir.value / "classpath.txt"
      val pw = new PrintWriter( classpath_file )
      val jars_list = customJars.getPaths.mkString("\n")
      pw.write( jars_list )
      customJars.classpath
    case None =>
      PathFinder.empty.classpath
  }
}

//unmanagedJars in Compile ++= {
//  sys.env.get("SPARK_HOME") match {
//    case Some(spark_dir) =>  ( file(spark_dir) ** "*.jar" ).classpath
//    case None => PathFinder.empty.classpath
//  }
//}

unmanagedClasspath in Test ++= Seq( conda_lib_dir.value )
unmanagedClasspath in (Compile, runMain) ++= Seq( conda_lib_dir.value )
classpathTypes += "dylib"
classpathTypes += "so"

stage ~= { (file: File) => cdas2Patch( file / "bin" / "cdas2" ); file }
// lazy val cdasGlobalCollectionsFile = settingKey[File]("The cdas global Collections file")

cdas_cache_dir := getCacheDir()
cdasPropertiesFile := cdas_cache_dir.value / "cdas.properties"
cdasDefaultPropertiesFile := baseDirectory.value / "project" / "cdas.properties"
cdasPythonRunScript := cdas_sbin_dir.value / "startup_python_worker.sh"
cdasDefaultPythonRunScript := baseDirectory.value / "bin" / "startup_python_worker.sh"
cdasStandaloneRunScript := cdas_sbin_dir.value / "startup_cdas_standalone.sh"
cdasDefaultStandaloneRunScript := baseDirectory.value / "bin" / "startup_cdas_standalone.sh"
cdasPythonShutdownScript := cdas_sbin_dir.value / "shutdown_python_worker.sh"
cdasDefaultPythonShutdownScript := baseDirectory.value / "bin" / "shutdown_python_worker.sh"
cdasSetupScript := cdas_sbin_dir.value / "setup_runtime.sh"
cdasDefaultSetupScript := baseDirectory.value / "bin" / "setup_runtime.sh"

cdasProperties := {
  val prop = new Properties()
  try{
     println("Loading property file: " + cdasPropertiesFile.value.toString )
    IO.load( prop, cdasPropertiesFile.value )
  } catch {
    case err: Exception => println("No property file found: " + cdasPropertiesFile.value.toString )
  }
  prop
}

upscr := {
  if( !cdasPropertiesFile.value.exists() ) {
    println("Copying default property file: " + cdasDefaultPropertiesFile.value.toString )
    copy( cdasDefaultPropertiesFile.value.toPath, cdasPropertiesFile.value.toPath )
  }
  println("Copying default python run script: " + cdasDefaultPythonRunScript.value.toString)
  copy( cdasDefaultPythonRunScript.value.toPath, cdasPythonRunScript.value.toPath, StandardCopyOption.REPLACE_EXISTING )
  println("Copying default python shutdown script: " + cdasDefaultPythonShutdownScript.value.toString )
  copy( cdasDefaultPythonShutdownScript.value.toPath, cdasPythonShutdownScript.value.toPath, StandardCopyOption.REPLACE_EXISTING )
  println("Copying default cdas spark-cluster startup script: " + cdasDefaultStandaloneRunScript.value.toString  + " to " + cdasStandaloneRunScript.value.toString )
  copy( cdasDefaultStandaloneRunScript.value.toPath, cdasStandaloneRunScript.value.toPath, StandardCopyOption.REPLACE_EXISTING )
  if( !cdasSetupScript.value.exists() ) {
    println("Copying default setup script: " + cdasDefaultSetupScript.value.toString )
    copy( cdasDefaultSetupScript.value.toPath, cdasSetupScript.value.toPath )
  }
}

compile  <<= (compile in Compile).dependsOn(upscr)

def getCondaLibDir(): File = sys.env.get("CONDA_PREFIX") match {
  case Some(ldir) => file(ldir) / "lib"
  case None => throw new Exception( "Must activate the cdas2 environment in Anaconda: '>> source activate cdas2' ")
}

def getCacheDir(): File = {
  val cache_dir = sys.env.get("CDAS_CACHE_DIR") match {
    case Some(cache_dir) => file(cache_dir)
    case None => file(System.getProperty("user.home")) / ".cdas" / "cache";
  }
  val ncml_dir = cache_dir / "collections" / "NCML";
  ncml_dir.mkdirs();
  cache_dir
}

def getCDASbinDir(): File = {
  val bin_dir =  file(System.getProperty("user.home")) / ".cdas" / "sbin";
  bin_dir.mkdirs();
  bin_dir
}

def getCDASlogsDir(): File = {
  val log_dir =  file(System.getProperty("user.home")) / ".cdas" / "logs";
  log_dir.mkdirs();
  log_dir
}

cdasLocalCollectionsFile :=  {
  val collections_file = cdas_cache_dir.value / "local_collections.xml"
  if( !collections_file.exists ) { xml.XML.save( collections_file.getAbsolutePath, <collections></collections> ) }
  collections_file
}

//cdasGlobalCollectionsFile := {
//  val collections_file = baseDirectory.value / "src" / "main" / "resources" / "global_collections.xml"
//  val collections_install_path = cdas_cache_dir.value / "global_collections.xml"
//  if( !collections_install_path.exists() ) { copy( collections_file.toPath, collections_install_path.toPath ) }
//  collections_install_path
//}

publishTo := Some(Resolver.file( "file",  sys.env.get("SBT_PUBLISH_DIR") match {
  case Some(pub_dir) => { val pdir = file(pub_dir); pdir.mkdirs(); pdir }
  case None =>  { val pdir = getCacheDir() / "publish"; pdir.mkdirs(); pdir }
} ) )

//
//md := {
//  import nasa.nccs.cds2.engine.MetadataPrinter
//}






