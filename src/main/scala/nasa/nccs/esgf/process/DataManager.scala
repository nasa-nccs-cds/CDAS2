package nasa.nccs.esgf.process
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.kernels.{AxisIndices, Kernel}
import ucar.nc2.dataset.CoordinateAxis1D
import ucar.{ma2, nc2}

import collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.concurrent
import scala.concurrent.Future

object FragmentSelectionCriteria extends Enumeration { val Largest, Smallest = Value }

trait DataLoader {
  def getDataset( collection: String, varName: String ): CDSDataset
  def getVariable( collection: String, varName: String ): CDSVariable
  def getFragment( fragSpec: DataFragmentSpec, abortSizeFraction: Float=0f ): PartitionedFragment
  def findEnclosingFragSpecs( fKeyChild: DataFragmentKey, admitEquality: Boolean = true): Set[DataFragmentKey]
  def findEnclosedFragSpecs( fKeyParent: DataFragmentKey, admitEquality: Boolean = true): Set[DataFragmentKey]
}

trait ScopeContext {
  def getConfiguration: Map[String,String]
  def config( key: String, default: String ): String = getConfiguration.getOrElse(key,default)
  def config( key: String ): Option[String] = getConfiguration.get(key)
}

class RequestContext( val domains: Map[String,DomainContainer], val inputs: Map[String, OperationInputSpec], val targetGrid: TargetGrid, private val configuration: Map[String,String] ) extends ScopeContext {
  def getConfiguration = configuration
  def missing_variable(uid: String) = throw new Exception("Can't find Variable '%s' in uids: [ %s ]".format(uid, inputs.keySet.mkString(", ")))
  def getDataSources: Map[String, OperationInputSpec] = inputs
  def getInputSpec( uid: String = "" ): OperationInputSpec = inputs.get( uid ) match {
    case Some(inputSpec) => inputSpec
    case None => inputs.head._2
  }
  def getDataset( serverContext: ServerContext, uid: String = "" ): CDSDataset = inputs.get( uid ) match {
    case Some(inputSpec) => inputSpec.data.getDataset(serverContext)
    case None =>inputs.head._2.data.getDataset(serverContext)
  }
  def getSection( serverContext: ServerContext, uid: String = "" ): ma2.Section = inputs.get( uid ) match {
    case Some(inputSpec) => inputSpec.data.roi
    case None =>inputs.head._2.data.roi
  }
  def getAxisIndices( uid: String ): AxisIndices = inputs.get(uid) match {
    case Some(inputSpec) => inputSpec.axes
    case None => missing_variable(uid)
  }
  def getDomain(domain_id: String): DomainContainer = domains.get(domain_id) match {
    case Some(domain_container) => domain_container
    case None => throw new Exception("Undefined domain in ExecutionContext: " + domain_id)
  }
}

case class OperationInputSpec( data: DataFragmentSpec, axes: AxisIndices ) {
  def getRange( dimension_name: String ): Option[ma2.Range] = data.getRange( dimension_name )
}

class TargetGrid( val variable: CDSVariable, val coordAxes: List[ CoordinateAxis1D ], val sectionOpt: Option[ma2.Section] ) {
//  val grid = coordAxes.indices.map( iDim => )
  def getCoordAxis( dimIndex: Int ): CoordinateAxis1D = coordAxes( dimIndex  )
  def getRange( dimIndex: Int ): ma2.Range = sectionOpt match { case Some(section) => section.getRange( dimIndex ); case None => variable.getFullSection.getRange( dimIndex ) }
}

class GridCoordSpec( val coordVar: nc2.Variable, val range: ma2.Range ) {
  private lazy val _data = coordVar.read( List(range) )
  def getData: ma2.Array = _data
}

class ServerContext( val dataLoader: DataLoader, private val configuration: Map[String,String] )  extends ScopeContext {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def getConfiguration = configuration
  def inputs( inputSpecs: List[OperationInputSpec] ): List[KernelDataInput] = for( inputSpec <- inputSpecs ) yield new KernelDataInput( getVariableData(inputSpec.data), inputSpec.axes )
  def getVariable(fragSpec: DataFragmentSpec ): CDSVariable = dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
  def getVariable(collection: String, varname: String ): CDSVariable = dataLoader.getVariable( collection, varname )
  def getVariableData( fragSpec: DataFragmentSpec ): PartitionedFragment = dataLoader.getFragment( fragSpec )

  def getAxisData( fragSpec: DataFragmentSpec, axis: Char ): ( Int, ma2.Array ) = {
    val variable: CDSVariable = dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
    val coordAxis = variable.dataset.getCoordinateAxis( axis )
    val axisIndex = variable.ncVariable.findDimensionIndex(coordAxis.getShortName)
    val range = fragSpec.roi.getRange( axisIndex )
    ( axisIndex -> coordAxis.read( List(range) ) )
  }

  def createTargetGrid( dataContainer: DataContainer, domainContainerOpt: Option[DomainContainer] ) = {
    val roiOpt: Option[List[DomainAxis]] = domainContainerOpt.map( domainContainer => domainContainer.axes )
    val source = dataContainer.getSource
    val variable: CDSVariable = dataLoader.getVariable( source.collection, source.name )
    val coordAxes: List[ CoordinateAxis1D ] = variable.getCoordinateAxes
    val sectionOpt: Option[ma2.Section] = roiOpt.map( roi => variable.getSubSection(roi) )
    new TargetGrid( variable, coordAxes, sectionOpt )
  }

  def getDataset(collection: String, varname: String ): CDSDataset = dataLoader.getDataset( collection, varname )



//  def getAxes( fragSpec: DataFragmentSpec ) = {
//    val variable: CDSVariable = dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
//    for( range <- fragSpec.roi.getRanges ) {
//      variable.getCoordinateAxis()
//    }
//
//  }
//    val dataset: CDSDataset = getDataset( fragSpec.collection,  fragSpec.varname  )
//    val coordAxes = dataset.getCoordinateAxes
// //   val newCoordVars: List[GridCoordSpec] = (for (coordAxis <- coordAxes) yield inputSpec.getRange(coordAxis.getShortName) match {
// //     case Some(range) => Some( new GridCoordSpec( coordAxis, range, ) ) )
////      case None => None
////    }).flatten
//    dataset.getCoordinateAxes
//  }
  def computeAxisSpecs( fragSpec: DataFragmentSpec, axisConf: List[OperationSpecs] ): AxisIndices = {
    val variable: CDSVariable = getVariable(fragSpec)
    variable.getAxisIndices( axisConf )
  }

  def getSubset( fragSpec: DataFragmentSpec, new_domain_container: DomainContainer ): PartitionedFragment = {
    val t0 = System.nanoTime
    val baseFragment = dataLoader.getFragment( fragSpec )
    val t1 = System.nanoTime
    val variable = getVariable( fragSpec )
    val newFragmentSpec = variable.createFragmentSpec( variable.getSubSection(new_domain_container.axes),  new_domain_container.mask )
    val rv = baseFragment.cutIntersection( newFragmentSpec.roi )
    val t2 = System.nanoTime
    logger.info( " GetSubsetT: %.4f %.4f".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
    rv
  }


//
//  def getSubset(uid: String, domain_container: DomainContainer): PartitionedFragment = {
//    uidToSource.get(uid) match {
//      case Some(dataSource) =>
//        dataSource.getData match {
//          case None => throw new Exception("Can't find data fragment for data source:  %s " + dataSource.toString)
//          case Some(fragment) => fragment.cutIntersection(getVariable(dataSource).getSubSection(domain_container.axes), true)
//        }
//      case None => missing_variable(uid)
//    }
//  }
//


  def loadVariableData( dataContainer: DataContainer, domain_container_opt: Option[DomainContainer], targetGrid: TargetGrid ): (String, OperationInputSpec) = {
    val data_source: DataSource = dataContainer.getSource
    val t0 = System.nanoTime
    val variable: CDSVariable = dataLoader.getVariable(data_source.collection, data_source.name)
    val t1 = System.nanoTime
    val axisSpecs: AxisIndices = variable.getAxisIndices( dataContainer.getOpSpecs )
    val t2 = System.nanoTime
    val maskOpt: Option[String] = domain_container_opt.map( domain_container => domain_container.mask ).flatten
    val fragmentSpec = targetGrid.sectionOpt match {
      case Some(section) =>
        val fragSpec: DataFragmentSpec = variable.createFragmentSpec( section, maskOpt )
        dataLoader.getFragment( fragSpec, 0.3f )
        fragSpec
      case None=> variable.createFragmentSpec( variable.getFullSection, maskOpt )
    }
    val t3 = System.nanoTime
    logger.info( " loadVariableDataT: %.4f %.4f ".format( (t1-t0)/1.0E9, (t3-t2)/1.0E9 ) )
    return ( dataContainer.uid -> new OperationInputSpec( fragmentSpec, axisSpecs )  )
  }
}


