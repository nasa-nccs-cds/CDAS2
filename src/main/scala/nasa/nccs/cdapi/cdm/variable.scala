package nasa.nccs.cdapi.cdm

import nasa.nccs.caching.Partitions
import nasa.nccs.cdapi.kernels.CDASExecutionContext
import nasa.nccs.cdapi.tensors.{CDByteArray, CDFloatArray, CDIndexMap}
import nasa.nccs.esgf.process._
import ucar.{ma2, nc2, unidata}
import ucar.nc2.dataset.{CoordinateAxis1D, _}
import nasa.nccs.utilities.Loggable
import ucar.nc2.constants.AxisType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

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
      { for( dim: nc2.Dimension <- dims; name=dim.getFullName; dlen=dim.getLength ) yield getCoordinateAxis( name ) match {
          case None=> <dimension name={name} length={dlen.toString}/>
          case Some(axis)=>
              val units = axis.getAxisType match { case AxisType.Time => "days since 1970-1-1" case x => axis.getUnitsString }
              <dimension name={name} length={dlen.toString} start={axis.getStart.toString} units={units} step={axis.getIncrement.toString} cfname={axis.getAxisType.getCFAxisName}/>
        }
      }
      { for( name <- attributes.keys ) yield <attribute name={name}> { getAttributeValue(name) } </attribute> }
    </variable>


  def read( section: ma2.Section ) = ncVariable.read(section)
  def getTargetGrid( fragSpec: DataFragmentSpec ): TargetGrid = fragSpec.targetGridOpt match { case Some(targetGrid) => targetGrid;  case None => new TargetGrid( this, Some(fragSpec.getAxes) ) }
  def getCoordinateAxes: List[ CoordinateAxis1D ] = {
    ncVariable.getDimensions.flatMap( dim => Option(dataset.ncDataset.findCoordinateAxis( dim.getFullName )).map( coordAxis => CDSVariable.toCoordAxis1D( coordAxis ) ) ).toList
  }
  def getCoordinateAxis( axisType: nc2.constants.AxisType ): Option[CoordinateAxis1D] = Option(dataset.ncDataset.findCoordinateAxis(axisType)).map( coordAxis => CDSVariable.toCoordAxis1D( coordAxis ) )
  def getCoordinateAxis( fullName: String ): Option[CoordinateAxis1D] = Option(dataset.ncDataset.findCoordinateAxis(fullName)).map( coordAxis => CDSVariable.toCoordAxis1D( coordAxis ) )
  def getCoordinateAxesList = dataset.getCoordinateAxes
}

abstract class OperationInput( val fragmentSpec: DataFragmentSpec, val metadata: Map[String,nc2.Attribute] ) extends Loggable {
  def toBoundsString = fragmentSpec.toBoundsString
  def getKey: DataFragmentKey = fragmentSpec.getKey
  def getKeyString: String = fragmentSpec.getKeyString
  def size: Int = fragmentSpec.roi.computeSize.toInt
  def contains( requestedSection: ma2.Section ): Boolean = fragmentSpec.roi.contains( requestedSection )
  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] = { fragmentSpec.getVariableMetadata(serverContext) ++ metadata }
  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] = { fragmentSpec.getDatasetMetadata(serverContext) }

  def domainDataFragment( partIndex: Int, context: CDASExecutionContext ): Option[DataFragment]
  def data(partIndex: Int ): CDFloatArray
  def delete
}

class PartitionedFragment( partitions: Partitions, val maskOpt: Option[CDByteArray], fragSpec: DataFragmentSpec, mdata: Map[String,nc2.Attribute] = Map.empty ) extends OperationInput(fragSpec,mdata)  {
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def delete = partitions.delete

  def data(partIndex: Int ): CDFloatArray = partitions.getPartData(partIndex, fragmentSpec.missing_value )

  def partFragSpec( partIndex: Int ): DataFragmentSpec = {
    val part = partitions.getPart(partIndex)
    fragmentSpec.reSection( fragmentSpec.roi.replaceRange(0, new ma2.Range( part.startIndex, part.startIndex + part.partSize -1 ) ) )
  }

  def domainFragSpec( partIndex: Int ): DataFragmentSpec = {
    val part = partitions.getPart(partIndex)
    fragmentSpec.domainSpec.reSection( fragmentSpec.roi.replaceRange(0, new ma2.Range( part.startIndex, part.startIndex + part.partSize -1 ) ) )
  }

  def partDataFragment( partIndex: Int ): DataFragment = {
    val partition = partitions.getPart(partIndex)
    new DataFragment( partFragSpec(partIndex), partition.data( fragmentSpec.missing_value ) )
  }

  def domainDataFragment( partIndex: Int, context: CDASExecutionContext ): Option[DataFragment] = {
    val optargs: Map[String, String] = context.operation.getConfiguration
    val optSection: Option[ma2.Section] = optargs.get("domain").flatMap(domainId => context.request.targetGrid.grid.getSubSection(context.request.getDomain(domainId).axes))
    try {
      val partition = partitions.getPart(partIndex)
      val partition_data = partition.data(fragmentSpec.missing_value)
      val frag_section = partition.partSection(fragmentSpec.roi)
      val domain_section = fragmentSpec.domainSectOpt match {
        case Some(dsect) => frag_section.intersect(dsect)
        case None => frag_section
      }
      val partFragSpec = domainFragSpec(partIndex)
      val sub_section = optSection match {
        case Some(osect) => domain_section.intersect(osect)
        case None => domain_section
      }
      partFragSpec.cutIntersection( sub_section ) match {
        case Some( cut_spec ) =>
          val array_section = cut_spec.roi.shiftOrigin( frag_section )
          Some(new DataFragment( cut_spec, CDFloatArray( partition_data.section( array_section ) ) ) )
        case None =>None
      }
    } catch {
      case ex: Exception =>
        logger.warn( s"Failed getting data fragment $partIndex: " + ex.toString )
        //        logger.error( ex.getStackTrace.mkString("\n\t") )
        None
    }
  }



      //      val domainDataOpt: Option[CDFloatArray] = fragmentSpec.domainSectOpt match {
//        case None => Some( partition.data(fragmentSpec.missing_value) )
//        case Some(domainSect) =>
//          val pFragSpec = partFragSpec( partIndex )
//          pFragSpec.cutIntersection(domainSect) match {
//            case Some(newFragSpec) =>
//              val dataSection = partition.getRelativeSection( newFragSpec.roi ).shiftOrigin( domainSect )
//              logger.info ("Domain Partition(%d) Fragment: fragSect=(%s), newFragSect=(%s), domainSect=(%s), dataSection=(%s), partition.shape=(%s)".format (partIndex, pFragSpec.roi.toString, newFragSpec.roi, domainSect.toString, dataSection.toString, partition.shape.mkString(",")) )
//              Some( partition.data (fragmentSpec.missing_value).section (dataSection.getRanges.toList) )
//            case None =>
//              logger.warn( "Domain Partition(%d) EMPTY INTERSECTION: fragSect=(%s), domainSect=(%s)".format (partIndex, pFragSpec.roi.toString, domainSect.toString) )
//              None
//          }
//      }
//      domainDataOpt.map( new DataFragment(domainFragSpec(partIndex), _ ) )


  def isMapped(partIndex: Int): Boolean = partitions.getPartData( partIndex, fragmentSpec.missing_value ).isMapped
  def mask: Option[CDByteArray] = maskOpt
  def shape: List[Int] = partitions.getShape.toList
  def getValue(partIndex: Int, indices: Array[Int] ): Float = data(partIndex).getValue( indices )

  override def toString = { "{Fragment: shape = [%s], section = [%s]}".format( partitions.getShape.mkString(","), fragmentSpec.roi.toString ) }

  def cutIntersection( partIndex: Int, cutSection: ma2.Section, copy: Boolean = true ): Option[DataFragment] = {
    val pFragSpec = partFragSpec( partIndex )
    pFragSpec.cutIntersection(cutSection) map { newFragSpec =>
        val newDataArray: CDFloatArray = data (partIndex).section (newFragSpec.roi.shiftOrigin (pFragSpec.roi).getRanges.toList)
        new DataFragment ( newFragSpec, if (copy) newDataArray.dup () else newDataArray )
    }
  }
}

object sectionTest1 extends App {
  val offset = new ma2.Section( Array( 20, 0, 0 ), Array( 0, 0, 0 ) )
  val section = new ma2.Section( Array( 20, 50, 30 ), Array( 100, 100, 100 ) )
  val section1 = section.compose( offset )
  println( section1.toString )
}
