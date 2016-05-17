package nasa.nccs.cdapi.cdm

import java.util.Formatter

import nasa.nccs.cdapi.kernels.AxisIndices
import nasa.nccs.cdapi.tensors.{CDArray, CDByteArray, CDFloatArray}
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.utilities.cdsutils
import ucar.nc2.time.{CalendarDate, CalendarDateRange}
import nasa.nccs.esgf.process._
import ucar.{ma2, nc2, unidata}
import ucar.nc2.dataset.{CoordinateAxis1D, _}
import ucar.nc2.constants.AxisType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable


object BoundsRole extends Enumeration { val Start, End = Value }

class KernelDataInput( val dataFragment: PartitionedFragment, val axisIndices: AxisIndices ) {
  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] =  dataFragment.getVariableMetadata(serverContext)
  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] =  dataFragment.getDatasetMetadata(serverContext)
  def getSpec: DataFragmentSpec = dataFragment.fragmentSpec
}

class AxisSpec( val name: String, val axisType: AxisType, val units: String ) {
  def toXml: xml.Elem = <axis id={name} units={units} class={axisClass} type={axisType.toString}> { getValues } </axis>
  val axisClass = "base"
  def getValues: String = ""
}
class RegularAxisSpec( name: String, axisType: AxisType, units: String, val start:Double, val interval:Double, val length: Int ) extends AxisSpec( name,  axisType, units) {
  override val axisClass = "regular"
  override def toXml: xml.Elem = <axis id={name} units={units} class={axisClass} type={axisType.toString} start={start.toString} interval={interval.toString} length={length.toString}> </axis>
}

class IregularAxisSpec( name: String, axisType: AxisType, units: String, val values: Array[Double] ) extends AxisSpec( name,  axisType, units) {
  override val axisClass = "iregular"
  override def getValues: String = values.mkString(", ")
}

class NominalAxisSpec( name: String, axisType: AxisType, units: String, val values: Array[String] ) extends AxisSpec( name,  axisType, units)  {
  override val axisClass = "nominal"
  override def getValues: String = values.mkString(", ")
}

class GridSpec( val axes: List[AxisSpec] ) {
  def toXml: xml.Elem = <grid> { axes.map(_.toXml) } </grid>
}

object CDSVariable { }

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

  def getCoordinateAxes: List[ CoordinateAxis1D ] = {
    ncVariable.getDimensions.map( dim => toCoordAxis1D( dataset.ncDataset.findCoordinateAxis( dim.getFullName ) ) ).toList
  }
  def getCoordinateAxis( axisType: nc2.constants.AxisType ): CoordinateAxis1D = toCoordAxis1D( dataset.ncDataset.findCoordinateAxis(axisType) )
  def getCoordinateAxis( fullName: String ): CoordinateAxis1D = toCoordAxis1D( dataset.ncDataset.findCoordinateAxis(fullName) )

  def getBoundedCalDate(coordAxis1DTime: CoordinateAxis1DTime, caldate: CalendarDate, role: BoundsRole.Value, strict: Boolean = true): CalendarDate = {
    val date_range: CalendarDateRange = coordAxis1DTime.getCalendarDateRange
    if (!date_range.includes(caldate)) {
      if (strict) throw new IllegalStateException("CDS2-CDSVariable: Time value %s outside of time bounds %s".format(caldate.toString, date_range.toString))
      else {
        if (role == BoundsRole.Start) {
          val startDate: CalendarDate = date_range.getStart
          logger.warn("Time value %s outside of time bounds %s, resetting value to range start date %s".format(caldate.toString, date_range.toString, startDate.toString))
          startDate
        } else {
          val endDate: CalendarDate = date_range.getEnd
          logger.warn("Time value %s outside of time bounds %s, resetting value to range end date %s".format(caldate.toString, date_range.toString, endDate.toString))
          endDate
        }
      }
    } else caldate
  }

  def getTimeAxis( coordAxis: CoordinateAxis): CoordinateAxis1DTime = coordAxis match {
    case coordAxis1DTime: CoordinateAxis1DTime => coordAxis1DTime
    case variableDS: VariableDS => CoordinateAxis1DTime.factory( dataset.ncDataset, variableDS, new Formatter() )
    case x => throw new IllegalStateException("CDS2-CDSVariable: Can't create time axis from type type: %s ".format(coordAxis.getClass.getName))
  }

  def getTimeCoordIndex(coordAxis: CoordinateAxis, tval: String, role: BoundsRole.Value, strict: Boolean = true): Int = {
    val coordAxis1DTime: CoordinateAxis1DTime = getTimeAxis( coordAxis )
    val caldate: CalendarDate = cdsutils.dateTimeParser.parse(tval)
    val caldate_bounded: CalendarDate = getBoundedCalDate(coordAxis1DTime, caldate, role, strict)
    coordAxis1DTime.findTimeIndexFromCalendarDate(caldate_bounded)
  }

  def getTimeIndexBounds( coordAxis: CoordinateAxis, startval: String, endval: String, strict: Boolean = true): ma2.Range = {
    val startIndex = getTimeCoordIndex( coordAxis, startval, BoundsRole.Start, strict)
    val endIndex = getTimeCoordIndex( coordAxis, endval, BoundsRole.End, strict)
    new ma2.Range( coordAxis.getAxisType.getCFAxisName, startIndex, endIndex)
  }

  def getNormalizedCoordinate(coordAxis: CoordinateAxis, cval: Double ): Double = coordAxis.getAxisType match {
    case nc2.constants.AxisType.Lon =>
      if( (cval<0.0) && ( coordAxis.getMinValue >= 0.0 ) ) cval + 360.0
      else if( (cval>180.0) && ( coordAxis.getMaxValue <= 180.0 ) ) cval - 360.0
      else cval
    case x => cval
  }

  def getNumericCoordValues( axis: CoordinateAxis1D, range: ma2.Range ): Array[Double] = {
    val coord_values = axis.getCoordValues;
    if (coord_values.length == range.length) coord_values else (0 until range.length).map(iE => coord_values(range.element(iE))).toArray
  }

  def getAxisSpec( coordAxis: CoordinateAxis1D, range: ma2.Range ): AxisSpec = {
    coordAxis.getAxisType match {
      case AxisType.Time =>
        val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(dataset.ncDataset, coordAxis, new Formatter())
        val timeCalValues: List[CalendarDate] = timeAxis.getCalendarDates.toList
        val timeZero = CalendarDate.of(timeCalValues.head.getCalendar,1970,1,1,1,1,1)
        val timeRelValues = for( index <- (range.first() to range.last() by range.stride()); calVal = timeCalValues(index) ) yield calVal.getDifferenceInMsecs(timeZero)/1000.0
        new IregularAxisSpec( "time", AxisType.Time, "seconds since 1970-1-1", timeRelValues.toArray )
      case x =>
        if (coordAxis.isRegular) new RegularAxisSpec(coordAxis.getFullName, coordAxis.getAxisType, coordAxis.getUnitsString, coordAxis.getCoordValue(range.first), coordAxis.getIncrement, range.length)
        else {
          if (coordAxis.isNumeric) new IregularAxisSpec(coordAxis.getFullName, coordAxis.getAxisType, coordAxis.getUnitsString, getNumericCoordValues(coordAxis, range))
          else throw new Exception("Nominal Coordinate types not yet supported: %s ".format(coordAxis.getFullName))
        }
      }
  }

  def getAxisSpecs( section: ma2.Section ): List[AxisSpec] =
    for( axisRangeTup <- getCoordinateAxes zip section.getRanges)
      yield getAxisSpec( axisRangeTup._1, axisRangeTup._2 )

  def getGridSpec( section: ma2.Section ): GridSpec = new GridSpec( getAxisSpecs(section) )

  def toCoordAxis1D(coordAxis: CoordinateAxis): CoordinateAxis1D = coordAxis match {
    case coordAxis1D: CoordinateAxis1D => coordAxis1D
    case _ => throw new IllegalStateException("CDSVariable: 2D Coord axes not yet supported: " + coordAxis.getClass.getName)
  }

  def getGridCoordIndex(coordAxis: CoordinateAxis, cval: Double, role: BoundsRole.Value, strict: Boolean = true): Int = {
    val coordAxis1D = toCoordAxis1D( coordAxis )
    coordAxis1D.findCoordElement( getNormalizedCoordinate( coordAxis, cval ) ) match {
      case -1 =>
        if (role == BoundsRole.Start) {
          val grid_start = coordAxis1D.getCoordValue(0)
          logger.warn("Axis %s: ROI Start value %f outside of grid area, resetting to grid start: %f".format(coordAxis.getShortName, cval, grid_start))
          0
        } else {
          val end_index = coordAxis1D.getSize.toInt - 1
          val grid_end = coordAxis1D.getCoordValue(end_index)
          logger.warn("Axis %s: ROI Start value %s outside of grid area, resetting to grid end: %f".format(coordAxis.getShortName, cval, grid_end))
          end_index
        }
      case ival => ival
    }
  }

  def getGridIndexBounds(coordAxis: CoordinateAxis, startval: Double, endval: Double, strict: Boolean = true): ma2.Range = {
    val startIndex = getGridCoordIndex(coordAxis, startval, BoundsRole.Start, strict)
    val endIndex = getGridCoordIndex(coordAxis, endval, BoundsRole.End, strict)
    new ma2.Range( coordAxis.getAxisType.getCFAxisName, startIndex, endIndex )
  }

  def getIndexBounds( coordAxis: CoordinateAxis, startval: GenericNumber, endval: GenericNumber, strict: Boolean = true): ma2.Range = {
    val indexRange = if (coordAxis.getAxisType == nc2.constants.AxisType.Time) getTimeIndexBounds( coordAxis, startval.toString, endval.toString ) else getGridIndexBounds(coordAxis, startval, endval)
    assert(indexRange.last >= indexRange.first, "CDS2-CDSVariable: Coordinate bounds appear to be inverted: start = %s, end = %s".format(startval.toString, endval.toString))
    indexRange
  }

  def getCFAxisName( dimension_index: Int, default_val: String ): String = {
    val dim: nc2.Dimension = ncVariable.getDimension(dimension_index)
    dataset.findCoordinateAxis(dim.getFullName) match {
      case Some(axis) => axis.getAxisType.getCFAxisName
      case None => default_val
    }
  }

  def getSubSection( roi: List[DomainAxis] ): ma2.Section = {
    val shape = ncVariable.getRanges.to[mutable.ArrayBuffer]
    for (axis <- roi) {
      dataset.getCoordinateAxis(axis.axistype) match {
        case Some(coordAxis) =>
          ncVariable.findDimensionIndex(coordAxis.getShortName) match {
            case -1 => throw new IllegalStateException("CDS2-CDSVariable: Can't find axis %s in variable %s".format(coordAxis.getShortName, ncVariable.getNameAndDimensions))
            case dimension_index =>
              axis.system match {
                case asys if asys.startsWith( "ind" ) =>
                  val axisName = coordAxis.getAxisType.getCFAxisName
                  shape.update(dimension_index, new ma2.Range( axisName, axis.start.toInt, axis.end.toInt, 1))
                case asys if asys.startsWith( "val" ) =>
                  val boundedRange = getIndexBounds(coordAxis, axis.start, axis.end)
                  shape.update(dimension_index, boundedRange)
                case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + axis.system)
              }
          }
        case None => logger.warn("Ignoring bounds on %s axis in variable %s".format(axis.name, ncVariable.getNameAndDimensions))
      }
    }
    shape.indices.map( iD => shape.update(iD, new ma2.Range( getCFAxisName( iD, shape(iD).getName ), shape(iD).first, shape(iD).last, 1)) )
    new ma2.Section( shape )
  }

  def createFragmentSpec( section: ma2.Section, mask: Option[String] = None, partIndex: Int=0, partAxis: Char='*', nPart: Int=1 ) = {
    val partitions = if ( partAxis == '*' ) List.empty[PartitionSpec] else {
      val partitionAxis = dataset.getCoordinateAxis(partAxis)
      val partAxisIndex = ncVariable.findDimensionIndex(partitionAxis.getShortName)
      assert(partAxisIndex != -1, "CDS2-CDSVariable: Can't find axis %s in variable %s".format(partitionAxis.getShortName, ncVariable.getNameAndDimensions))
      List( new PartitionSpec(partAxisIndex, nPart, partIndex) )
    }
    new DataFragmentSpec( name, dataset.name, ncVariable.getDimensionsString(), ncVariable.getUnitsString, getAttributeValue( "long_name", ncVariable.getFullName ), section, mask, partitions.toArray )
  }

  def createFragmentSpec() = new DataFragmentSpec( name, dataset.name )

  def loadPartition( fragmentSpec : DataFragmentSpec, axisConf: List[OperationSpecs] ): PartitionedFragment = {
    val partition = fragmentSpec.partitions.head
    val sp = new SectionPartitioner(fragmentSpec.roi, partition.nPart)
    sp.getPartition(partition.partIndex, partition.axisIndex ) match {
      case Some(partSection) =>
        val array = ncVariable.read(partSection)
        val cdArray: CDFloatArray = CDFloatArray.factory(array, missing )
        createPartitionedFragment( fragmentSpec, cdArray )
      case None =>
        logger.warn("No fragment generated for partition index %s out of %d parts".format(partition.partIndex, partition.nPart))
        new PartitionedFragment()
    }
  }

  def loadRoi( fragmentSpec: DataFragmentSpec ): PartitionedFragment = {
    val array: ma2.Array = ncVariable.read(fragmentSpec.roi)
    val cdArray: CDFloatArray = CDFloatArray.factory(array, missing )
//    ncVariable.getDimensions.foreach( dim => println( "Dimension %s: %s ".format( dim.getShortName,  dim.writeCDL(true) )))
    createPartitionedFragment( fragmentSpec, cdArray )
  }

  def getAxisIndices( axisConf: List[OperationSpecs] ): AxisIndices = {
    val axis_ids = mutable.HashSet[Int]()
    for( opSpec <- axisConf ) {
      val axes = opSpec.getSpec("axes")
      val axis_chars: List[Char] = if( axes.contains(',') ) axes.split(",").map(_.head).toList else axes.toList
      axis_ids ++= axis_chars.map( cval => getAxisIndex( cval ) )
    }
    new AxisIndices( axisIds=axis_ids.toSet )
  }

  def getAxisIndex( axisClass: Char ): Int = {
    val coord_axis = dataset.getCoordinateAxis(axisClass)
    ncVariable.findDimensionIndex( coord_axis.getShortName )
  }

  def createPartitionedFragment( fragmentSpec: DataFragmentSpec, maskOpt: Option[CDByteArray], ndArray: CDFloatArray ): PartitionedFragment =  {
    new PartitionedFragment( ndArray, maskOpt, fragmentSpec )
  }

}

class PartitionedFragment( array: CDFloatArray, val maskOpt: Option[CDByteArray], val fragmentSpec: DataFragmentSpec, val metaData: (String, String)*  ) {
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def this() = this( new CDFloatArray( Array(0), Array.emptyFloatArray, Float.MaxValue ), new DataFragmentSpec )

  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] = {
    fragmentSpec.getVariableMetadata(serverContext) ++ Map( metaData.map( item => (item._1 -> new nc2.Attribute(item._1,item._2)) ) :_* )
  }
  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] = {
    fragmentSpec.getDatasetMetadata(serverContext)
  }

  def data: CDFloatArray = array
  def mask: Option[CDByteArray] = maskOpt
  def shape: List[Int] = array.getShape.toList
  def getValue( indices: Array[Int] ): Float = array.getValue( indices )

  override def toString = { "{Fragment: shape = [%s], section = [%s]}".format( array.getShape.mkString(","), fragmentSpec.roi.toString ) }

  def cutIntersection( cutSection: ma2.Section, copy: Boolean = true ): PartitionedFragment = {
    val newFragSpec = fragmentSpec.cutIntersection(cutSection)
    val newDataArray: CDFloatArray = array.section( newFragSpec.roi.shiftOrigin(fragmentSpec.roi).getRanges.toList )
    new PartitionedFragment( if(copy) newDataArray.dup else newDataArray, maskOpt, newFragSpec )
  }

  def cutNewSubset( newSection: ma2.Section, copy: Boolean = true ): PartitionedFragment = {
    if (fragmentSpec.roi.equals( newSection )) this
    else {
      val relativeSection = newSection.shiftOrigin( fragmentSpec.roi )
      val newDataArray: CDFloatArray = array.section( relativeSection.getRanges.toList )
      new PartitionedFragment( if(copy) newDataArray.dup else newDataArray, maskOpt, fragmentSpec.reSection( newSection ) )
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

