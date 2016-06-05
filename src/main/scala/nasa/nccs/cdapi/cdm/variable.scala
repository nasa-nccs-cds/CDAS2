package nasa.nccs.cdapi.cdm

import nasa.nccs.cdapi.kernels.AxisIndices
import nasa.nccs.cdapi.tensors.{CDArray, CDByteArray, CDFloatArray, CDIndexMap}
import nasa.nccs.cds2.engine.FragmentPersistence
import nasa.nccs.utilities.cdsutils
import ucar.nc2.time.{CalendarDate, CalendarDateRange}
import nasa.nccs.esgf.process._
import ucar.{ma2, nc2, unidata}
import ucar.nc2.dataset.{CoordinateAxis1D, _}
import ucar.nc2.constants.AxisType
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object BoundsRole extends Enumeration { val Start, End = Value }

class KernelDataInput( val dataFragment: PartitionedFragment, val axisIndices: AxisIndices ) {
  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] =  dataFragment.getVariableMetadata(serverContext)
  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] =  dataFragment.getDatasetMetadata(serverContext)
  def getSpec: DataFragmentSpec = dataFragment.fragmentSpec
}

object CDSVariable {
  def toCoordAxis1D(coordAxis: CoordinateAxis): CoordinateAxis1D = coordAxis match {
    case coordAxis1D: CoordinateAxis1D => coordAxis1D
    case _ => throw new IllegalStateException("CDSVariable: 2D Coord axes not yet supported: " + coordAxis.getClass.getName)
  }
}

class CDSVariable( val name: String, val dataset: CDSDataset, val ncVariable: nc2.Variable) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.cdm.CDSVariable")
  val description = ncVariable.getDescription
  val dims = ncVariable.getDimensionsAll.toList
  val units = ncVariable.getUnitsString
  val shape = ncVariable.getShape.toList
  val fullname = ncVariable.getFullName
  val attributes = nc2.Attribute.makeMap(ncVariable.getAttributes).toMap
  val missing = getAttributeValue( "missing_value", "" ) match { case "" => Float.MaxValue; case s => s.toFloat }

  def getFullSection: ma2.Section = ncVariable.getShapeAsSection
  def getAttributeValue( key: String, default_value: String  ) =  attributes.get( key ) match { case Some( attr_val ) => attr_val.toString.split('=').last; case None => default_value }
  override def toString = "\nCDSVariable(%s) { description: '%s', shape: %s, dims: %s, }\n  --> Variable Attributes: %s".format(name, description, shape.mkString("[", " ", "]"), dims.mkString("[", ",", "]"), attributes.mkString("\n\t\t", "\n\t\t", "\n"))
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase
  def getAttributeValue( name: String ): String =  attributes.getOrElse(name, new nc2.Attribute(new unidata.util.Parameter("",""))).getValue(0).toString
  def toXml: xml.Node =
    <variable name={name} fullname={fullname} description={description} shape={shape.mkString("[", " ", "]")} units={units}>
      { for( dim: nc2.Dimension <- dims; name=dim.getFullName; dlen=dim.getLength ) yield  <dimension name={name} length={dlen.toString}/>  }
      { for( name <- attributes.keys ) yield <attribute name={name}> { getAttributeValue(name) }</attribute> }
    </variable>
    // dims: %s, }\n  --> Variable Attributes: %s".format(name, description, shape.mkString("[", " ", "]"), dims.mkString("[", ",", "]"), attributes.mkString("\n\t\t", "\n\t\t", "\n"))


  def read( section: ma2.Section ) = ncVariable.read(section)
  def getTargetGrid( fragSpec: DataFragmentSpec ): TargetGrid = fragSpec.targetGridOpt match { case Some(targetGrid) => targetGrid;  case None => new TargetGrid( this, Some(fragSpec.getAxes) ) }
  def getCoordinateAxes: List[ CoordinateAxis1D ] = {
    ncVariable.getDimensions.map( dim => CDSVariable.toCoordAxis1D( dataset.ncDataset.findCoordinateAxis( dim.getFullName ) ) ).toList
  }
  def getCoordinateAxis( axisType: nc2.constants.AxisType ): CoordinateAxis1D = CDSVariable.toCoordAxis1D( dataset.ncDataset.findCoordinateAxis(axisType) )
  def getCoordinateAxis( fullName: String ): CoordinateAxis1D = CDSVariable.toCoordAxis1D( dataset.ncDataset.findCoordinateAxis(fullName) )

}

class PartitionedFragment( array: CDFloatArray, val maskOpt: Option[CDByteArray], val fragmentSpec: DataFragmentSpec, val metaData: (String, String)*  )  {
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)
//  private var dataStore: Option[ CDFloatArray ] = Some( array )
//  private val cdIndexMap: CDIndexMap = array.getIndex
//  private val invalid: Float = array.getInvalid

  def this() = this( CDFloatArray( Array(0), Array.emptyFloatArray, Float.MaxValue ), None, new DataFragmentSpec )

  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] = {
    fragmentSpec.getVariableMetadata(serverContext) ++ Map( metaData.map( item => (item._1 -> new nc2.Attribute(item._1,item._2)) ) :_* )
  }
  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] = {
    fragmentSpec.getDatasetMetadata(serverContext)
  }
  def data: CDFloatArray = array

//  def data: CDFloatArray = dataStore match {
//    case Some( array ) => array
//    case None => restore match {
//      case Some(array) => new CDFloatArray( cdIndexMap, array, invalid )
//      case None => throw new Exception( "Error restoring data for fragment: "+ fragmentSpec.toString )
//    }
//  }
//  def restore: Option[ Array[Float] ] = FragmentPersistence.getFragmentData( fragmentSpec: DataFragmentSpec )
//  def free = { dataStore = None }

  def mask: Option[CDByteArray] = maskOpt
  def shape: List[Int] = data.getShape.toList
  def getValue( indices: Array[Int] ): Float = data.getValue( indices )

  override def toString = { "{Fragment: shape = [%s], section = [%s]}".format( data.getShape.mkString(","), fragmentSpec.roi.toString ) }

  def cutIntersection( cutSection: ma2.Section, copy: Boolean = true ): PartitionedFragment = {
    val newFragSpec = fragmentSpec.cutIntersection(cutSection)
    val newDataArray: CDFloatArray = data.section( newFragSpec.roi.shiftOrigin(fragmentSpec.roi).getRanges.toList )
    new PartitionedFragment( if(copy) newDataArray.dup() else newDataArray, maskOpt, newFragSpec )
  }

  def cutNewSubset( newSection: ma2.Section, copy: Boolean = true ): PartitionedFragment = {
    if (fragmentSpec.roi.equals( newSection )) this
    else {
      val relativeSection = newSection.shiftOrigin( fragmentSpec.roi )
      val newDataArray: CDFloatArray = data.section( relativeSection.getRanges.toList )
      new PartitionedFragment( if(copy) newDataArray.dup() else newDataArray, maskOpt, fragmentSpec.reSection( newSection ) )
    }
  }
  def size: Long = fragmentSpec.roi.computeSize
  def contains( requestedSection: ma2.Section ): Boolean = fragmentSpec.roi.contains( requestedSection )
}

object sectionTest extends App {
  val s0 = new ma2.Section( Array(10,10,0), Array(100,100,10) )
  val s1 = new ma2.Section( Array(50,50,5), Array(10,10,1) )
  val s2 = s1.shiftOrigin( s0 )
  println( s2 )
}

