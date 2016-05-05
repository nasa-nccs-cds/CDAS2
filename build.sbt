val kernelPackages = settingKey[ Seq[String] ]("A list of user-defined Kernel packages")

name := "CDS2"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

organization := "nasa.nccs"

lazy val root = project in file(".")

//  ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
resolvers += "spray repo" at "http://repo.spray.io"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= Dependencies.scala

libraryDependencies ++= Dependencies.kernels

libraryDependencies ++= Dependencies.spark

libraryDependencies ++= Dependencies.cache




    