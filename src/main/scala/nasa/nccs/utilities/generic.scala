package nasa.nccs.utilities

import java.util.jar.JarFile
import com.joestelmach.natty
import ucar.nc2.time.CalendarDate
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.slf4j.Logger

trait Loggable {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
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

