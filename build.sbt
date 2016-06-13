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

externalDependencyClasspath in Runtime += baseDirectory.value / "cache"

fork in run:= true

javaOptions in run ++= Seq( "-Xmx2G", "-Xms512M")



    