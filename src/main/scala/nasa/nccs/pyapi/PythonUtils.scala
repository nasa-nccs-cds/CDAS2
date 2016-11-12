package nasa.nccs.pyapi

import java.io.File
import java.net.{URI, URISyntaxException}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

object PythonUtils {
  /** Get the PYTHONPATH for PySpark, either from SPARK_HOME, if it is set, or from our JAR */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]
    for (sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator)
      pythonPath += Seq(sparkHome, "python", "lib", "py4j-0.9-src.zip").mkString(File.separator)
    }
    pythonPath ++= SparkContext.jarOfObject(this)
    pythonPath.mkString(File.pathSeparator)
  }

  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val uri = resolveURI(p)
        Option(uri.getScheme).getOrElse("file") match {
          case "local" | "file" => false
          case _ => true
        }
      }
    }
  }

  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => resolveURI(p) }.mkString(",")
    }
  }
  /** Merge PYTHONPATHS with the appropriate separator. Ignores blank strings. */
  def mergePythonPaths(paths: String*): String = {
    paths.filter(_ != "").mkString(File.pathSeparator)
  }

  def generateRDDWithNull(sc: JavaSparkContext): JavaRDD[String] = {
    sc.parallelize(List("a", null, "b"))
  }

  /**
    * Convert list of T into seq of T (for calling API with varargs)
    */
  def toSeq[T](vs: JList[T]): Seq[T] = {
    vs.asScala
  }

  /**
    * Convert list of T into a (Scala) List of T
    */
  def toList[T](vs: JList[T]): List[T] = {
    vs.asScala.toList
  }

  /**
    * Convert list of T into array of T (for calling API with array)
    */
  def toArray[T](vs: JList[T]): Array[T] = {
    vs.toArray().asInstanceOf[Array[T]]
  }

  /**
    * Convert java map of K, V into Map of K, V (for calling API with varargs)
    */
  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] = {
    jm.asScala.toMap
  }
}
