package nasa.nccs.cdas.kernels
import com.google.common.reflect.ClassPath
import nasa.nccs.utilities.Loggable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.xml

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

object KernelModule extends Loggable {
  def apply( classInfoRecs: List[ClassInfoRec] ): KernelModule = {
    val kernelClassMap = Map(classInfoRecs.map(_.getMapEntry): _*)
    new KernelModule( classInfoRecs.head.module, kernelClassMap.mapValues(KernelModule.loadKernel(_) ) )
  }
  def apply(moduleSpec: String): KernelModule = {
    val specToks = moduleSpec.split("[!]")
    val api = specToks(1)
    val module_name = (api + "." + specToks(0)).toLowerCase()
    val kernelSpecs = specToks(2).split("[~]")
    val kernels = kernelSpecs.map(kspec => Kernel( module_name, kspec, api ) )
    new KernelModule( module_name, Map( kernels.map( kernel => kernel.operation.toLowerCase -> Some(kernel)): _*) )
  }

  def toXml( moduleSpec: String ): xml.Elem = {
    val specToks = moduleSpec.split("[!]")
    val kernelSpecs = specToks(2).split("[~]")
    <kernelModule name={specToks(0)} api={specToks(1)}>
      <kernels> { kernelSpecs.map( kernelSpec => getKernelXml(specToks(0),kernelSpec) ) } </kernels>
    </kernelModule>
  }

  def getKernelXml( modname: String, kernelSpec: String ): xml.Elem = {
      val specToks = kernelSpec.split("[;]")
      <kernel module={modname} name={specToks(0)} description={specToks(1)} inputs={specToks(1)} />
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
      logger.error( err.getStackTrace.mkString("\n") )
      None
  }
}

class KernelModule( val name: String, val kernels: Map[String,Option[Kernel]] ) extends Loggable {
  def getKernel(name: String): Option[Kernel] = kernels.get(name).flatten
  def getKernels: Iterable[Kernel] = kernels.values.flatten
  def getKernelNames: List[String] = kernels.keys.toList
  def getName = name

  def toXml: xml.Elem = {
    <kernelModule name={name}>
      <kernels> { kernels.values.flatMap( _.map( _.toXmlHeader )  ) } </kernels>
    </kernelModule>
  }
}



