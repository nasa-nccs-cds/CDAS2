package nasa.nccs.utilities

import java.io.{File, PrintWriter}
import java.lang.management.ManagementFactory
import java.util.jar.JarFile
import java.nio.file.{Files, Path, Paths}

import scala.xml
import com.joestelmach.natty
import ucar.nc2.time.CalendarDate
import java.nio.file.{Files, Path}

import nasa.nccs.esgf.process.UID

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

//object log4jInit {
//  import org.apache.log4j._
//  val console: ConsoleAppender = new ConsoleAppender();
//  val PATTERN = "%d [%p|%c|%C{1}] %m%n";
//  console.setLayout(new PatternLayout(PATTERN));
//  console.setThreshold(Level.FATAL);
//  console.activateOptions();
//  Logger.getRootLogger().addAppender(console);
//
//  val fa = new FileAppender();
//  fa.setName("FileLogger");
//  fa.setFile("${user.home}/.cdas/wps.log");
//  fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
//  fa.setThreshold(Level.DEBUG);
//  fa.setAppend(true);
//  fa.activateOptions();
//  Logger.getRootLogger().addAppender(fa);
//}

class Logger( val name: String, val test: Boolean, val master: Boolean ) extends Serializable {
  val LNAME = if( test ) name + "-test" else name + "-"
  val LID = if( master ) "master" else UID().uid
  val logFilePath: Path = Paths.get( System.getProperty("user.home"), ".cdas", "logs", LNAME + LID + ".log" )
  val writer = if(Files.exists(logFilePath)) {
    new PrintWriter(logFilePath.toString)
  } else {
    Files.createDirectories( logFilePath.getParent )
    new PrintWriter( new File( logFilePath.toString ) )
  }
  def log( level: String, msg: String  ) = {
    val output = level + ": " + msg
    writer.println( output )
    writer.flush()
    if(!test) { println( output ) }
  }
  def info( msg: String ) = { log( "info", msg ) }
  def debug( msg: String ) = { log( "debug", msg ) }
  def error( msg: String ) = { log( "error", msg ) }
  def warn( msg: String ) = { log( "warn", msg ) }
}


object CDASLogManager extends Serializable {
  private var _test = false
  private var _master = false
  lazy private val _logger: Logger = new Logger("cdas",_test,_master)
  def testing = { _test = true }
  def isMaster = { _master = true }
  def getCurrentLogger() = { _logger }

//  def getLogger( name: String ) = {
//    val console = new ConsoleAppender();
//    val PATTERN = "%d [%p|%c|%C{1}] %m%n";
//    console.setLayout(new PatternLayout(PATTERN));
//    console.setThreshold(Level.DEBUG);
//    console.activateOptions();
//    Logger.getRootLogger().addAppender(console);
//
//    val fa = new FileAppender();
//    fa.setName("FileLogger");
//    fa.setFile(  );
//    fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
//    fa.setThreshold(Level.DEBUG);
//    fa.setAppend(true);
//    fa.activateOptions();
//    Logger.getRootLogger().addAppender(fa);
//
//    Logger.getLogger( name )
//  }
}

trait Loggable extends Serializable {
  def logger = CDASLogManager.getCurrentLogger()

  def logError( err: Throwable, msg: String ) = {
    logger.error(msg)
    logger.error(err.getMessage)
    logger.error( err.getStackTrace.mkString("\n") )
  }
}

object cdsutils {

  val baseTimeUnits = "seconds since 1970-01-01T00:00:00Z"

  def getOrElse[T]( map: Map[String,T], key: String, errMsg: String ): T = map.get(key) match { case Some(x) => x; case None => throw new Exception(errMsg) }

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
  def getMult( ch: Char ): Long = ch.toLower match {
    case 'k' =>  1024
    case 'm' =>  1024 * 1024
    case 'g' =>  1024 * 1024 * 1024
    case 't' =>  1024 * 1024 * 1024 * 1024
  }

  def parseMemsize( msize: String ): Long = {
    val tmsize = msize.trim
    if( tmsize.last.isLetter ) {
      val n0 = tmsize.substring(0,tmsize.length-1).toDouble
      val m = getMult( tmsize.last )
      Math.round( n0 * m )
    }
    else tmsize.toInt
  }

  def ptime[R]( label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println( "%s: Time = %.4f s".format( label, (t1-t0)/1.0E9 ))
    result
  }


  def time[R](logger:Logger, label: String)(block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    logger.debug( "%s: Time = %.4f s".format( label, (t1-t0)/1.0E3 ))
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

/*
  // Getting past type erasure
import scala.reflect.runtime.universe._
def matchList[A: TypeTag](list: List[A]) = list match {
  case strlist: List[String @unchecked] if typeOf[A] =:= typeOf[String] => println("A list of strings!")
  case intlist: List[Int @unchecked] if typeOf[A] =:= typeOf[Int] => println("A list of ints!")
}
*/
