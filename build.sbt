val kernelPackages = settingKey[ Seq[String] ]("A list of user-defined Kernel packages")

name := "cdas2"

version := "1.2-SNAPSHOT"

scalaVersion := "2.11.7"

organization := "nasa.nccs"

lazy val root = project in file(".")

//  ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

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
lazy val cdasLocalCollectionsFile = settingKey[File]("The cdas local Collections file")

cdasPropertiesFile := baseDirectory.value / "project" / "cdas.properties"

cdasProperties := {
  val prop = new Properties()
  try{ IO.load( prop, cdasPropertiesFile.value ) } catch { case err: Exception => println("No property file found") }
  prop
}

def getCacheDir( properties: Properties ): File =
  sys.env.get("CDAS_CACHE_DIR") match {
    case Some(cache_dir) => file(cache_dir)
    case None =>
      val home = file(System.getProperty("user.home"))
      val cache_dir = properties.getProperty("cdas.cache.dir", "")
      if (cache_dir.isEmpty) { home / ".ivy2" / "local" } else file( cache_dir )
  }

lazy val cdas_cache_dir = settingKey[File]("The CDAS cache directory.")

def getPublishDir( properties: Properties ): File =
  sys.env.get("SBT_PUBLISH_DIR") match {
    case Some(pub_dir) => { val pdir = file(pub_dir); pdir.mkdirs(); pdir }
    case None =>
      val home = file(System.getProperty("user.home"))
      val cache_dir = properties.getProperty("cdas.publish.dir", "")
      if(cache_dir.isEmpty) { home / ".cdas" / "cache" } else file( cache_dir )
  }

cdas_cache_dir := {
  val cache_dir = getCacheDir( cdasProperties.value )
  cache_dir.mkdirs()
  cdasProperties.value.put( "cdas.cache.dir", cache_dir.getAbsolutePath )
  try{ IO.write( cdasProperties.value, "", cdasPropertiesFile.value ) } catch { case err: Exception => println("Error writing to properties file: " + err.getMessage ) }
  cache_dir
}

cdasLocalCollectionsFile :=  {
  val collections_file = cdas_cache_dir.value / "local_collections.xml"
  if( !collections_file.exists ) { xml.XML.save( collections_file.getAbsolutePath, <collections></collections> ) }
  collections_file
}

unmanagedClasspath in Compile += cdas_cache_dir.value
unmanagedClasspath in Runtime += cdas_cache_dir.value
unmanagedClasspath in Test += cdas_cache_dir.value

publishTo := Some(Resolver.file( "file", getPublishDir( cdasProperties.value ) ) )


//lazy val md = taskKey[Unit]("Prints 'Hello World'")
//
//md := {
//  import nasa.nccs.cds2.engine.MetadataPrinter
//}




    