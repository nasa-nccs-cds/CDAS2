package nasa.nccs.cdapi.cdm

import nasa.nccs.caching.{Partition, Partitions}
import nasa.nccs.cdapi.kernels.AxisIndices
import nasa.nccs.cdapi.tensors.{CDArray, CDByteArray, CDFloatArray, CDIndexMap}
import nasa.nccs.esgf.process._
import ucar.{ma2, nc2, unidata}
import ucar.nc2.dataset.{CoordinateAxis1D, _}
import nasa.nccs.utilities.Loggable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.xml.XML

object BoundsRole extends Enumeration { val Start, End = Value }

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
    ncVariable.getDimensions.flatMap( dim => Option(dataset.ncDataset.findCoordinateAxis( dim.getFullName )).map( coordAxis => CDSVariable.toCoordAxis1D( coordAxis ) ) ).toList
  }
  def getCoordinateAxis( axisType: nc2.constants.AxisType ): Option[CoordinateAxis1D] = Option(dataset.ncDataset.findCoordinateAxis(axisType)).map( coordAxis => CDSVariable.toCoordAxis1D( coordAxis ) )
  def getCoordinateAxis( fullName: String ): Option[CoordinateAxis1D] = Option(dataset.ncDataset.findCoordinateAxis(fullName)).map( coordAxis => CDSVariable.toCoordAxis1D( coordAxis ) )
  def getCoordinateAxesList = dataset.getCoordinateAxes
}

class PartitionedFragment( partitions: Partitions, val maskOpt: Option[CDByteArray], val fragmentSpec: DataFragmentSpec, val metaData: (String, String)*  ) extends Loggable  {
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)
//  private var dataStore: Option[ CDFloatArray ] = Some( array )
//  private val cdIndexMap: CDIndexMap = array.getIndex
//  private val invalid: Float = array.getInvalid

//  def this() = this( CDFloatArray( Array(0), Array.emptyFloatArray, Float.MaxValue ), None, new DataFragmentSpec() )
  def toBoundsString = fragmentSpec.toBoundsString

  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] = {
    fragmentSpec.getVariableMetadata(serverContext) ++ Map( metaData.map( item => (item._1 -> new nc2.Attribute(item._1,item._2)) ) :_* )
  }
  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] = {
    fragmentSpec.getDatasetMetadata(serverContext)
  }
  def data(partIndex: Int ): CDFloatArray = partitions.getPartData(partIndex, fragmentSpec.missing_value )

  def partFragSpec( partIndex: Int ): DataFragmentSpec = {
    val part = partitions.getPart(partIndex)
    fragmentSpec.reSection( fragmentSpec.roi.insertRange(0, new ma2.Range( part.startIndex, part.startIndex + part.partSize -1 ) ) )
  }

  def domainFragSpec( partIndex: Int ): DataFragmentSpec = {
    val part = partitions.getPart(partIndex)
    fragmentSpec.domainSpec.reSection( fragmentSpec.roi.insertRange(0, new ma2.Range( part.startIndex, part.startIndex + part.partSize -1 ) ) )
  }

  def partDataFragment( partIndex: Int ): DataFragment = {
    val partition = partitions.getPart(partIndex)
    new DataFragment( partFragSpec(partIndex), partition.data( fragmentSpec.missing_value ) )
  }

  def domainDataFragment( partIndex: Int ): Option[DataFragment] = {
    try {
      val partition = partitions.getPart(partIndex)
      val domainData = fragmentSpec.domainSectOpt match {
        case None => partition.data(fragmentSpec.missing_value);
        case Some(domainSect) =>
          val pFragSpec = partFragSpec( partIndex )
          val newFragSpec = pFragSpec.cutIntersection(domainSect)
          val dataSection = newFragSpec.roi.shiftOrigin(pFragSpec.roi)
          logger.info( "Domain Partition(%d) Fragment: dataSection=(%s), fragSect=(%s), domainSect=(%s)".format( partIndex, dataSection.toString, pFragSpec.roi.toString, domainSect.toString))
          partition.data(fragmentSpec.missing_value).section( dataSection.getRanges.toList )
      }
      Some( new DataFragment(domainFragSpec(partIndex), domainData) )
    } catch {
      case ex: Exception =>
        logger.warn( s"Failed getting data fragment $partIndex: " + ex.getMessage )
        None
    }
  }

  def isMapped(partIndex: Int): Boolean = partitions.getPartData( partIndex, fragmentSpec.missing_value ).isMapped

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
  def shape: List[Int] = partitions.getShape.toList
  def getValue(partIndex: Int, indices: Array[Int] ): Float = data(partIndex).getValue( indices )

  override def toString = { "{Fragment: shape = [%s], section = [%s]}".format( partitions.getShape.mkString(","), fragmentSpec.roi.toString ) }

  def cutIntersection( partIndex: Int, cutSection: ma2.Section, copy: Boolean = true ): DataFragment = {
    val pFragSpec = partFragSpec( partIndex )
    val newFragSpec = pFragSpec.cutIntersection(cutSection)
    val newDataArray: CDFloatArray = data(partIndex).section( newFragSpec.roi.shiftOrigin(pFragSpec.roi).getRanges.toList )
    new DataFragment( newFragSpec, if(copy) newDataArray.dup() else newDataArray )
  }

  def size: Int = fragmentSpec.roi.computeSize.toInt
  def contains( requestedSection: ma2.Section ): Boolean = fragmentSpec.roi.contains( requestedSection )
}

object sectionTest1 extends App {
  val offset = new ma2.Section( Array( 20, 0, 0 ), Array( 0, 0, 0 ) )
  val section = new ma2.Section( Array( 20, 50, 30 ), Array( 100, 100, 100 ) )
  val section1 = section.compose( offset )
  println( section1.toString )
}
