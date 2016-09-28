package nasa.nccs.cds2.kernels
import com.google.common.reflect.ClassPath
import nasa.nccs.cdapi.kernels.Kernel
import nasa.nccs.utilities.cdsutils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait KernelTools {

}

class ReflectionTools {
  import org.reflections._
  import org.reflections.scanners.{ResourcesScanner, SubTypesScanner}
  import org.reflections.util.{ClasspathHelper, ConfigurationBuilder, FilterBuilder}

  val kernelsPackage = "nasa.nccs.cds2.modules"
  val classLoadersList = List[ClassLoader](ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader() )
  val configuration = new ConfigurationBuilder().setScanners(new SubTypesScanner(false), new ResourcesScanner())
    .setUrls(ClasspathHelper.forClassLoader(classLoadersList:_*)).filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix( kernelsPackage )))

  val reflections: Reflections = new Reflections(configuration)

}

class ClassInfoRec( val module: String, val name: String, val classinfo: ClassPath.ClassInfo ) {
  def getMapEntry = ( name -> classinfo )
}

object KernelModule {
  def apply( classInfoRecs: List[ClassInfoRec] ): KernelModule = new KernelModule( Map( classInfoRecs.map( _.getMapEntry ): _* ) )
}

class KernelModule( val kernels: Map[String,ClassPath.ClassInfo] ) {
  def getKernelClassInfo(name: String): Option[ClassPath.ClassInfo] = kernels.get(name)
  def getKernelInstance(name: String): Option[Kernel] = kernels.get(name).map( cls => cls.load().getDeclaredConstructors()(0).newInstance().asInstanceOf[Kernel] )
}

object KernelPackageTools {
  val internalKernelsPackage = "nasa.nccs.cds2.modules"
  def apply(): KernelPackageTools = new KernelPackageTools( List(internalKernelsPackage) )
}

class KernelPackageTools( val kernelPackagePaths: List[String]) {
  import com.google.common.reflect.ClassPath
  val classpath = ClassPath.from( getClass.getClassLoader )

  def getKernelClasses: List[ClassPath.ClassInfo] = {
    kernelPackagePaths.map( package_path => classpath.getTopLevelClassesRecursive( package_path ).toList ).foldLeft(List[ClassPath.ClassInfo]())( _ ++ _ )
  }

  def getKernelMap: Map[String,KernelModule] = {
    getKernelClasses.map( kClassInfo => {
      val kernel_name = kClassInfo.getSimpleName
      val mod_name = kClassInfo.getPackageName.split('.').last
      new ClassInfoRec( mod_name, kernel_name, kClassInfo )
    }).groupBy( _.module ).mapValues( KernelModule(_) )
  }
}

object ClasspathToolsTest extends App {
  val kernelsPackage = "nasa.nccs.cds2.modules"
  val cptools = new KernelPackageTools( List(kernelsPackage) )
  val kmap = cptools.getKernelMap
  kmap.get("CDSpark") match {
    case Some( kmod ) =>
      println( "Got module ")
      kmod.getKernelInstance("min") match {
        case Some( kernel ) =>
          println( "Got kernel " + kernel.getClass.getName )
          cdsutils.testSerializable(kernel)
        case None => println( "No kernel ")
      }
    case None => println( "No Module ")
  }
}
