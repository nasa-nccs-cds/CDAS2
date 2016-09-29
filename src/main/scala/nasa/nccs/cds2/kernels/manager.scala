package nasa.nccs.cds2.kernels
import java.util.jar.JarFile

import nasa.nccs.cdapi.kernels.Kernel
import nasa.nccs.cds2.modules
import nasa.nccs.utilities.cdsutils
import nasa.nccs.cds2.modules.{CDS, CDSpark}

import collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => u}
import scala.reflect.runtime.universe._



class KernelMgr(  ) {

  val kernelModules = KernelPackageTools.getKernelMap

  def getModule( moduleName: String ): Option[KernelModule] = kernelModules.get( moduleName.toLowerCase )

  def getModuleNames: List[String] = kernelModules.keys.toList

//  def isKernelModuleJar(jarFile: JarFile): Boolean = cdsutils.getJarAttribute( jarFile, "Specification-Title" ) == "CDS2KernelModule"
//
//  def importKernelModuleSpecs(jarFile: JarFile): Iterator[Class[_]] =
//    for( cls <- cdsutils.getClassesFromJar(jarFile); if cls.getSuperclass.getName == "nasa.nccs.cdapi.kernels.KernelModuleSpec") yield cls // cls.getDeclaredConstructors()(0).newInstance().asInstanceOf[KernelModuleSpec]

  def toXml = <modules>{ kernelModules.values.map( _.toXml ) } </modules>

  def getModulesXml = {
    val elemList: List[xml.Elem] = kernelModules.values.map( _.toXml ).toList
    <kernels>{ elemList }</kernels>
  }

//  def collectKernelModules(): Map[String, KernelModule] = {
//    Map.empty
//    val kspecs = ( for (jarFile <- cdsutils.getProjectJars; if isKernelModuleJar(jarFile); kspec <- importKernelModuleSpecs(jarFile) ) yield  u.typeOf[kspec.type] ).toSeq
//    val kmodTypes = Seq( modules.CDS, CDSpark ) ++ kspecs
//    val instances = modules.CDSpark.getKernelInstances
//    Map.empty
//    val kernelItems = kmodTypes.map( kmodType => {
//      val kspec = kmodType.getClass().getDeclaredConstructors()(0).newInstance().asInstanceOf[KernelModuleSpec]
//      val kernelTags = kmodType.members // .filter( m => m.isClass )
//      val kernelInstances = kernelTags.flatMap ( _.getClass.getDeclaredConstructors()(0).newInstance() match { case kernel: Kernel => Some(kernel); case _ => None } )
//      val kernelMap =  Map( kernelInstances.map( kernel=> kernel.operation.toLowerCase -> kernel ).toSeq:_* )
//      kspec.name.toLowerCase -> new KernelModule( kspec, kernelMap )
//    })
//    Map( kernelItems: _* )
//  }

}



