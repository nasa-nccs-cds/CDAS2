package nasa.nccs.cds2.kernels
import com.google.common.reflect.ClassPath
import nasa.nccs.cdapi.kernels.Kernel
import nasa.nccs.utilities.Loggable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


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
  def getMapEntry = ( name.toLowerCase -> classinfo )
}

object KernelModule {
  def apply( classInfoRecs: List[ClassInfoRec] ): KernelModule = new KernelModule( classInfoRecs.head.module, Map( classInfoRecs.map( _.getMapEntry ): _* ) )

  def toXml( moduleSpec: String ): xml.Elem = {
    val specToks = moduleSpec.split("[!]")
    val kernelSpecs = specToks(1).split("[~]")
    <kernelModule name={specToks(0)}>
      <kernels> { kernelSpecs.map( kernelSpec => getKernelXml(specToks(0),kernelSpec) ) } </kernels>
    </kernelModule>
  }

  def getKernelXml( modname: String, kernelSpec: String ): xml.Elem = {
      val specToks = kernelSpec.split("[;]")
      <kernel module={modname} name={specToks(0)} description={specToks(1)} inputs={specToks(1)} />
  }
}

class KernelModule( val name: String, val kernelClassMap: Map[String,ClassPath.ClassInfo] ) extends Loggable {
  val kernels: Map[String,Option[Kernel]] = kernelClassMap.mapValues( loadKernel(_) )
  def getKernelClassInfo(name: String): Option[ClassPath.ClassInfo] = kernelClassMap.get(name)
  def getKernel(name: String): Option[Kernel] = kernels.get(name).flatten
  def getKernels: Iterable[Kernel] = kernels.values.flatten
  def getKernelNames: List[String] = kernels.keys.toList

  def toXml: xml.Elem = {
    <kernelModule name={name}>
      <kernels> { kernels.keys.map( kname => <kernel module={name} name={kname}/> ) } </kernels>
    </kernelModule>
  }

  def loadKernel( cls: ClassPath.ClassInfo ): Option[Kernel] = try {
    cls.load().getConstructor().newInstance() match {
      case kernel: Kernel => Some(kernel);
      case _ =>
        logger.error( "Error loading Kernel class-> Can't cast to Kernel: " + cls.getName )
        None
    }
  } catch {
    case err: Exception =>
      logger.error( "%s(%s) loading Kernel class: %s".format( err.getClass.getName, err.getMessage, cls.getName ) )
      None
  }

}



