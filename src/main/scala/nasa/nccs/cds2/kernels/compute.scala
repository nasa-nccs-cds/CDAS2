package nasa.nccs.cds2.kernels
import com.google.common.reflect.ClassPath
import nasa.nccs.cdapi.kernels.Kernel
import nasa.nccs.utilities.cdsutils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait KernelTools {

}

// TOUSE: Include dependency  'reflections'
//class ReflectionTools {
//  import org.reflections._
//  import org.reflections.scanners.{ResourcesScanner, SubTypesScanner}
//  import org.reflections.util.{ClasspathHelper, ConfigurationBuilder, FilterBuilder}
//
//  val kernelsPackage = "nasa.nccs.cds2.modules"
//  val classLoadersList = List[ClassLoader](ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader() )
//  val configuration = new ConfigurationBuilder().setScanners(new SubTypesScanner(false), new ResourcesScanner())
//    .setUrls(ClasspathHelper.forClassLoader(classLoadersList:_*)).filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix( kernelsPackage )))
//
//  val reflections: Reflections = new Reflections(configuration)
//
//}

object ClassInfoRec {
  def apply( classinfo: ClassPath.ClassInfo ): ClassInfoRec = new ClassInfoRec( classinfo.getPackageName.split('.').last, classinfo.getSimpleName, classinfo )
}
class ClassInfoRec( val module: String, val name: String, val classinfo: ClassPath.ClassInfo ) {
  def getMapEntry = ( name -> classinfo )
}

object KernelModule {
  def apply( classInfoRecs: List[ClassInfoRec] ): KernelModule = new KernelModule( classInfoRecs.head.module, Map( classInfoRecs.map( _.getMapEntry ): _* ) )
}

class KernelModule( val name: String, val kernels: Map[String,ClassPath.ClassInfo] ) {
//  val spec = kernels.values.head.load().getPackage().getClass().
  def getKernelClassInfo(name: String): Option[ClassPath.ClassInfo] = kernels.get(name)
  def getKernel(name: String): Option[Kernel] = kernels.get(name).flatMap( cls => cls.load().getDeclaredConstructors()(0).newInstance() match { case kernel: Kernel => Some(kernel); case _ => None } )
  def getKernelNames: List[String] = kernels.keys.toList

  def toXml = {
    <kernelModule name={name}>
      <kernels> { kernels.keys.map( kname => <kernel name={kname}/> ) } </kernels>
    </kernelModule>
  }
}



object KernelPackageTools {
  import com.google.common.reflect.ClassPath
  val internalKernelsPackage = "nasa.nccs.cds2.modules"
  val externalKernelPackages = cdsutils.envList("CDAS_KERNEL_PACKAGES")
  val classpath = ClassPath.from( getClass.getClassLoader )
  val kernelPackagePaths: List[String] = List( internalKernelsPackage ) ++ externalKernelPackages

  def getKernelClasses: List[ClassPath.ClassInfo] = {
    kernelPackagePaths.map( package_path => classpath.getTopLevelClassesRecursive( package_path ).toList ).foldLeft(List[ClassPath.ClassInfo]())( _ ++ _ )
  }

  def getKernelMap: Map[String,KernelModule] = {
    getKernelClasses.map(ClassInfoRec( _ )).groupBy( _.module.toLowerCase ).mapValues( KernelModule(_) )
  }
}

object ClasspathToolsTest extends App {
  val kmap = KernelPackageTools.getKernelMap
  kmap.get("CDSpark") match {
    case Some( kmod ) =>
      println( "Got module ")
      kmod.getKernel("min") match {
        case Some( kernel ) =>
          println( "Got kernel " + kernel.getClass.getName )
          cdsutils.testSerializable(kernel)
        case None => println( "No kernel ")
      }
    case None => println( "No Module ")
  }
}
