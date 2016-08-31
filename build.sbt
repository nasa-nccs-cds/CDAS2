import java.nio.file.Files.copy
import java.nio.file.Paths.get
import sbt._

val kernelPackages = settingKey[ Seq[String] ]("A list of user-defined Kernel packages")

name := "cdas2"

version := "1.2-SNAPSHOT"

scalaVersion := "2.11.7"

organization := "nasa.nccs"

lazy val root = project in file(".")

resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
resolvers += "Java.net repository" at "http://download.java.net/maven/2"
resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"
resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Boundless Maven Repository" at "http://repo.boundlessgeo.com/main"
resolvers += "spray repo" at "http://repo.spray.io"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= Dependencies.scala

libraryDependencies ++= Dependencies.spark

libraryDependencies ++= Dependencies.cache

libraryDependencies ++= Dependencies.geo

libraryDependencies ++= Dependencies.netcdf

fork in run:= true
fork in test:= true
logBuffered in Test := false

javaOptions in run ++= Seq( "-Xmx64000M", "-Xms512M")

import java.util.Properties

lazy val cdasProperties = settingKey[Properties]("The cdas properties map")
lazy val cdasPropertiesFile = settingKey[File]("The cdas properties file")
lazy val cdasDefaultPropertiesFile = settingKey[File]("The cdas defaultproperties file")
lazy val cdasLocalCollectionsFile = settingKey[File]("The cdas local Collections file")
lazy val cdas_cache_dir = settingKey[File]("The CDAS cache directory.")

cdas_cache_dir := { val cache_dir = getCacheDir();  cache_dir.mkdirs();  cache_dir  }
cdasPropertiesFile := cdas_cache_dir.value / "cdas.properties"
cdasDefaultPropertiesFile := baseDirectory.value / "project" / "cdas.properties"

//  try{ IO.write( cdasProperties.value, "", cdasPropertiesFile.value ) } catch { case err: Exception => println("Error writing to properties file: " + err.getMessage ) }

cdasProperties := {
  val prop = new Properties()
  try{
    if( !cdasPropertiesFile.value.exists() ) {
      println("Copying default property file: " + cdasDefaultPropertiesFile.value.toString )
      copy( cdasDefaultPropertiesFile.value.toPath, cdasPropertiesFile.value.toPath )
    }
    println("Loading property file: " + cdasPropertiesFile.value.toString )
    IO.load( prop, cdasPropertiesFile.value )
  } catch {
    case err: Exception => println("No property file found")
  }
  prop
}

def getCacheDir(): File =
  sys.env.get("CDAS_CACHE_DIR") match {
    case Some(cache_dir) => file(cache_dir)
    case None => file(System.getProperty("user.home")) / ".cdas" / "cache"
  }


cdasLocalCollectionsFile :=  {
  val collections_file = cdas_cache_dir.value / "local_collections.xml"
  if( !collections_file.exists ) { xml.XML.save( collections_file.getAbsolutePath, <collections></collections> ) }
  collections_file
}

unmanagedClasspath in Compile += cdas_cache_dir.value
unmanagedClasspath in Runtime += cdas_cache_dir.value
unmanagedClasspath in Test += cdas_cache_dir.value

publishTo := Some(Resolver.file( "file",  sys.env.get("SBT_PUBLISH_DIR") match {
  case Some(pub_dir) => { val pdir = file(pub_dir); pdir.mkdirs(); pdir }
  case None =>
    val pub_dir = cdasProperties.value.getProperty("publish.dir", "")
    if(pub_dir.isEmpty) { cdas_cache_dir.value } else { val pdir = file(pub_dir); pdir.mkdirs(); pdir }
} ) )


//lazy val md = taskKey[Unit]("Prints 'Hello World'")
//
//md := {
//  import nasa.nccs.cds2.engine.MetadataPrinter
//}




