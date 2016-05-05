package nasa.nccs.cds2.kernels
import java.util.jar.JarFile
import nasa.nccs.cdapi.kernels.KernelModule
import nasa.nccs.utilities.cdsutils
import collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class KernelMgr(  ) {

  val kernelModules = collectKernelModules()

  def getModule( moduleName: String ): Option[KernelModule] = kernelModules.get( moduleName.toLowerCase )

  def getModuleNames: List[String] = kernelModules.keys.toList

  def isKernelModuleJar(jarFile: JarFile): Boolean = cdsutils.getJarAttribute( jarFile, "Specification-Title" ) == "CDS2KernelModule"

  def getKernelModules(jarFile: JarFile): Iterator[KernelModule] =
    for( cls <- cdsutils.getClassesFromJar(jarFile); if cls.getSuperclass.getName == "nasa.nccs.cdapi.kernels.KernelModule") yield cls.getDeclaredConstructors()(0).newInstance().asInstanceOf[KernelModule]

  def toXml = <modules>{ kernelModules.values.map( _.toXml ) } </modules>

  def collectKernelModules(): Map[String, KernelModule] = {
    val kernelModules = new mutable.HashMap[String, KernelModule]()
    val cds = new nasa.nccs.cds2.modules.CDS.CDS()
    kernelModules += (cds.name.toLowerCase -> cds)
    for (jarFile <- cdsutils.getProjectJars; if isKernelModuleJar(jarFile); kmod <- getKernelModules(jarFile) ) kernelModules += (kmod.name -> kmod)
    kernelModules.toMap
  }
}



