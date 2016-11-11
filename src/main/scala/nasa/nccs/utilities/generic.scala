package nasa.nccs.utilities

import java.util.jar.JarFile
import com.joestelmach.natty
import ucar.nc2.time.CalendarDate
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.log4j.{ Logger, LogManager, Level }

object CDASLogManager extends Serializable {
  val logger = getLogger

  def getLogger: Logger = {
    val _logger: Logger = LogManager.getLogger(this.getClass)
    _logger.setLevel(Level.INFO)
    _logger
  }
}

trait Loggable extends Serializable {

  def logger = CDASLogManager.logger

  def logError( err: Throwable, msg: String ) = {
    logger.error(msg)
    logger.error(err.getMessage)
    logger.error( err.getStackTrace.mkString("\n") )
  }

}

object cdsutils {

  val baseTimeUnits = "days since 1970-1-1"

  def flatlist[T]( values: Option[T]* ): List[T] = values.flatten.toList

  def ceilDiv( numer: Int, denom: Int ) : Int = Math.ceil( numer/ denom.toFloat ).toInt

  def getInstance[T]( cls: Class[T] ) = cls.getConstructor().newInstance()

  def findNonNull[T]( values: T* ): Option[T] = values.toList.find( _ != null )

  def cdata(obj: Any): String = "<![CDATA[\n " + obj.toString + "\n]]>"

  def isValid(obj: Any): Boolean = Option(obj) match { case Some(x) => true; case None => false }

  def toString( value: Any, max_len: Int = 250 ): String = { val vstr = value.toString; if( vstr.length > max_len ) vstr.substring(0,max_len) else vstr }

  def attributeValueEquals(value: String)(node: xml.Node) = node.attributes.exists(_.value.text == value)

  def getProjectJars: Array[JarFile] = {
    import java.io.File
    val cpitems = System.getProperty("java.class.path").split(File.pathSeparator)
    for ( cpitem <- cpitems; fileitem = new File(cpitem); if fileitem.isFile && fileitem.getName.toLowerCase.endsWith(".jar") ) yield new JarFile(fileitem)
  }

  def envList(name: String): Array[String] =
    try { sys.env(name).split(':') }
    catch { case ex: java.util.NoSuchElementException => Array.empty[String] }

  def testSerializable( test_object: AnyRef ) = {
    import java.io._
    val out = new ObjectOutputStream(new FileOutputStream("test.obj"))
    val name = test_object.getClass.getSimpleName
    try {
      out.writeObject(test_object)
      println( s" ** SER +++ '$name'" )
    } catch {
      case ex: java.io.NotSerializableException => println( s" ** SER --- '$name'" )
    } finally {
      out.close
    }
  }

  def printHeapUsage = {
    val MB = 1024 * 1024
    val heapSize: Long = Runtime.getRuntime.totalMemory
    val heapSizeMax: Long = Runtime.getRuntime.maxMemory
    val heapFreeSize: Long = Runtime.getRuntime.freeMemory
    println( "-->> HEAP: heapSize = %d M, heapSizeMax = %d M, heapFreeSize = %d M".format( heapSize/MB, heapSizeMax/MB, heapFreeSize/MB ) )

  }

  def ptime[R]( label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println( "%s: Time = %.4f s".format( label, (t1-t0)/1.0E9 ))
    result
  }


  def time[R](logger:Logger, label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    logger.debug( "%s: Time = %.4f s".format( label, (t1-t0)/1.0E9 ))
    result
  }

  def getJarAttribute(jarFile: JarFile, attribute_name: String ): String = {
    val manifest = jarFile.getManifest
    if( isValid(manifest) ) manifest.getMainAttributes.getValue(attribute_name) else ""
  }

  def getClassesFromJar(jarFile: JarFile): Iterator[Class[_]] = {
    import java.net.{URL, URLClassLoader}, java.util.jar.JarEntry
    val cloader: URLClassLoader = URLClassLoader.newInstance(Array(new URL("jar:file:" + jarFile.getName + "!/")))
    for (je: JarEntry <- jarFile.entries; ename = je.getName; if ename.endsWith(".class");
         cls = cloader.loadClass(ename.substring(0, ename.length - 6).replace('/', '.')) ) yield cls
  }


  object dateTimeParser {
    import com.joestelmach.natty
    private val parser = new natty.Parser()

    def parse(input: String): CalendarDate = {
      val caldates = mutable.ListBuffer[CalendarDate]()
      val groups = parser.parse(input).toList
      for (group: natty.DateGroup <- groups; date: java.util.Date <- group.getDates.toList) caldates += CalendarDate.of(date)
      assert( caldates.size == 1, " DateTime Parser Error: parsing '%s'".format(input) )
      caldates.head
    }
  }

  //  def loadExtensionModule( jar_file: String, module: Class ): Unit = {
  //    var classLoader = new java.net.URLClassLoader( Array(new java.io.File( jar_file ).toURI.toURL ), this.getClass.getClassLoader)
  //    var clazzExModule = classLoader.loadClass(module.GetClass.GetName + "$") // the suffix "$" is for Scala "object",
  //    try {
  //      //"MODULE$" is a trick, and I'm not sure about "get(null)"
  //      var module = clazzExModule.getField("MODULE$").get(null).asInstanceOf[module]
  //    } catch {
  //      case e: java.lang.ClassCastException =>
  //        printf(" - %s is not Module\n", clazzExModule)
  //    }
  //
  //  }
}

object stringFixer extends App {
  val instr = """ 295.6538,295.7205,295.9552,295.3324,293.0879,291.5541,289.6255,288.7875,289.7614,290.5001,292.3553,
                |  293.8378,296.7862,296.6005,
                |  295.6378,
                |  294.9304,
                |  293.6324,
                |  292.1851,
                |  290.8981,
                |  290.5262,290.5347,
                |  291.6595,
                |  292.8715,
                |  294.0839,
                |  295.4386,
                |  296.1736,
                |  296.4382,
                |  294.7264,
                |  293.0489,
                |  291.6237,
                |  290.5149,
                |  290.1141,
                |  289.8373,
                |  290.8802,
                |  292.615,
                |  294.0024,
                |  295.5854,
                |  296.5497,
                |  296.4013,
                |  295.1263,
                |  293.2203,
                |  292.2885,
                |  291.0839,
                |  290.281,
                |  290.1516,
                |  290.7351,
                |  292.7598,
                |  294.1442,
                |  295.8959,
                |  295.8112,
                |  296.1058,
                |  294.8028,
                |  292.7733,
                |  291.7613,
                |  290.7009,
                |  290.7226,
                |  290.1038,
                |  290.6277,
                |  292.1299,
                |  294.4099,
                |  296.1226,
                |  296.5852,
                |  296.4395,
                |  294.7828,
                |  293.7856,
                |  291.9353,
                |  290.2696,
                |  289.8393,
                |  290.3558,
                |  290.162,
                |  292.2701,
                |  294.3617,
                |  294.6855,
                |  295.9736,
                |  295.9881,
                |  294.853,
                |  293.4628,
                |  292.2583,
                |  291.2488,
                |  290.84,
                |  289.9593,
                |  290.8045,
                |  291.5576,
                |  293.0114,
                |  294.7605,
                |  296.3679,
                |  295.6986,
                |  293.4995,
                |  292.2574,
                |  290.9722,
                |  289.9694,
                |  290.1006,
                |  290.2442,
                |  290.7669,
                |  292.0513,
                |  294.2266,
                |  295.9346,
                |  295.6064,
                |  295.4227,
                |  294.3889,
                |  292.8391 """
  val newStr = instr.replace('\n',' ').replace('|',' ').replace(" ","")
  print(newStr)


}

