package nasa.nccs.esgf.process
import java.util.Formatter

import nasa.nccs.caching._
import nasa.nccs.cdapi.cdm._
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D, CoordinateAxis1DTime, VariableDS}
import java.util.Formatter

import nasa.nccs.cdapi.kernels.AxisIndices
import nasa.nccs.cdapi.tensors.{CDArray, CDByteArray, CDDoubleArray, CDFloatArray}
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.utilities.{Loggable, cdsutils}
import ucar.nc2.time.{CalendarDate, CalendarDateRange}
import ucar.{ma2, nc2}
import ucar.nc2.constants.AxisType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

sealed abstract class DataAccessMode
object DataAccessMode {
  case object Read extends DataAccessMode
  case object Cache extends DataAccessMode
  case object MetaData extends DataAccessMode
}

object FragmentSelectionCriteria extends Enumeration { val Largest, Smallest = Value }

trait DataLoader {
  def getDataset( collection: Collection, varname: String ): CDSDataset
  def getVariable( collection: Collection, varname: String ): CDSVariable
  def getExistingFragment( fragSpec: DataFragmentSpec  ): Option[Future[PartitionedFragment]]
  def cacheFragmentFuture( fragSpec: DataFragmentSpec  ): Future[PartitionedFragment];
}

trait ScopeContext {
  def getConfiguration: Map[String,String]
  def config( key: String, default: String ): String = getConfiguration.getOrElse(key,default)
  def config( key: String ): Option[String] = getConfiguration.get(key)
}

class RequestContext( val domains: Map[String,DomainContainer], val inputs: Map[String, DataFragmentSpec], val targetGrid: TargetGrid, private val configuration: Map[String,String] ) extends ScopeContext {
  def getConfiguration = configuration
  def missing_variable(uid: String) = throw new Exception("Can't find Variable '%s' in uids: [ %s ]".format(uid, inputs.keySet.mkString(", ")))
  def getDataSources: Map[String, DataFragmentSpec] = inputs
  def getInputSpec( uid: String = "" ): DataFragmentSpec = inputs.get( uid ) match {
    case Some(inputSpec) => inputSpec
    case None => inputs.head._2
  }
  def getDataset( serverContext: ServerContext, uid: String = "" ): CDSDataset = inputs.get( uid ) match {
    case Some(inputSpec) => inputSpec.getDataset(serverContext)
    case None =>inputs.head._2.getDataset(serverContext)
  }
  def getSection( serverContext: ServerContext, uid: String = "" ): ma2.Section = inputs.get( uid ) match {
    case Some(inputSpec) => inputSpec.roi
    case None =>inputs.head._2.roi
  }
  def getDomain(domain_id: String): DomainContainer = domains.get(domain_id) match {
    case Some(domain_container) => domain_container
    case None => throw new Exception("Undefined domain in ExecutionContext: " + domain_id)
  }
  def getAxisIndices( axisConf: String ): AxisIndices = targetGrid.getAxisIndices( axisConf  )
}

class GridCoordSpec( val index: Int, val variable: CDSVariable, val coordAxis: CoordinateAxis1D, val domainAxisOpt: Option[DomainAxis] ) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.cdm.GridCoordSpec")
  private val range: ma2.Range = getAxisRange( variable, coordAxis, domainAxisOpt )
  private val _data = getCoordinateValues
  val bounds: Array[Double] = getAxisBounds( coordAxis, domainAxisOpt)
  def getData: Array[Double] = _data
  def getAxisType: AxisType = coordAxis.getAxisType

  def getCFAxisName: String = Option(coordAxis.getAxisType) match  {
    case Some( axisType ) => getAxisType.getCFAxisName
    case None => logger.warn( "Using %s for CFAxisName".format(coordAxis.getShortName) ); coordAxis.getShortName
  }
  def getAxisName: String = coordAxis.getFullName
  def getIndexRange: ma2.Range = range
  def getLength: Int = _data.length
  def getStartValue: Double = bounds(0)
  def getEndValue: Double = bounds(1)
  def toXml: xml.Elem = <axis id={getAxisName} units={getUnits} cfName={getCFAxisName} type={getAxisType.toString} start={getStartValue.toString} end={getEndValue.toString} length={getLength.toString} > </axis>

  private def getAxisRange( variable: CDSVariable, coordAxis: CoordinateAxis, domainAxisOpt: Option[DomainAxis]): ma2.Range = domainAxisOpt match {
    case Some( domainAxis ) =>  domainAxis.system match {
      case asys if asys.startsWith("ind") =>
        new ma2.Range( getCFAxisName, domainAxis.start.toInt, domainAxis.end.toInt, 1 )
      case asys if asys.startsWith("val") =>
        getGridIndexBounds( domainAxis.start, domainAxis.end )
      case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + domainAxis.system)
    }
    case None => new ma2.Range( getCFAxisName, 0, coordAxis.getShape(0)-1, 1 )
  }
  private def getAxisBounds( coordAxis: CoordinateAxis, domainAxisOpt: Option[DomainAxis]): Array[Double] = domainAxisOpt match {
    case Some( domainAxis ) =>  domainAxis.system match {
      case asys if asys.startsWith("val") => Array( _data(0), _data(_data.length-1) )
      case asys if asys.startsWith("ind") => Array( domainAxis.start.toDouble, domainAxis.end.toDouble, 1 )
      case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + domainAxis.system)
    }
    case None => Array( coordAxis.getMinValue, coordAxis.getMaxValue )
  }

  def getBounds( range: ma2.Range ): Array[Double] = Array( _data(range.first), _data(range.last ) )

  def getBoundedCalDate(coordAxis1DTime: CoordinateAxis1DTime, caldate: CalendarDate, role: BoundsRole.Value, strict: Boolean = false): CalendarDate = {
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

  def getCoordinateValues: Array[Double] = {
    coordAxis.getAxisType match {
      case AxisType.Time =>
        variable.dataset.getDatasetFileHeaders match {
          case Some(datasetFileHeaders: DatasetFileHeaders) => datasetFileHeaders.getAggAxisValues
          case None =>
            val sec_in_day = 60 * 60 * 24
            val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(variable.dataset.ncDataset, coordAxis, new Formatter())
            val timeCalValues: List[CalendarDate] = timeAxis.getCalendarDates.toList
            val timeZero = CalendarDate.of(timeCalValues.head.getCalendar, 1970, 1, 1, 1, 1, 1)
            val time_values = for (index <- (range.first() to range.last() by range.stride()); calVal = timeCalValues(index)) yield (calVal.getDifferenceInMsecs(timeZero) / 1000).toDouble / sec_in_day
            time_values.toArray[Double]
        }
      case x =>
        CDDoubleArray.factory( coordAxis.read(List(range)) ).getArrayData()
    }
  }

  def getUnits: String =  coordAxis.getAxisType match {
    case AxisType.Time => "days since 1970-1-1"
    case x => coordAxis.getUnitsString
  }

  def getTimeAxis: CoordinateAxis1DTime = coordAxis match {
    case coordAxis1DTime: CoordinateAxis1DTime => coordAxis1DTime
    case variableDS: VariableDS => CoordinateAxis1DTime.factory( variable.dataset.ncDataset, variableDS, new Formatter() )
    case x => throw new IllegalStateException("CDS2-CDSVariable: Can't create time axis from type type: %s ".format(coordAxis.getClass.getName))
  }

  def getTimeCoordIndex( tval: String, role: BoundsRole.Value, strict: Boolean = false): Int = {
    val coordAxis1DTime: CoordinateAxis1DTime = getTimeAxis
    val caldate: CalendarDate = cdsutils.dateTimeParser.parse(tval)
    val caldate_bounded: CalendarDate = getBoundedCalDate(coordAxis1DTime, caldate, role, strict)
    coordAxis1DTime.findTimeIndexFromCalendarDate(caldate_bounded)
  }

  def getTimeIndexBounds( startval: String, endval: String, strict: Boolean = false): ma2.Range = {
    val startIndex = getTimeCoordIndex( startval, BoundsRole.Start, strict)
    val endIndex = getTimeCoordIndex( endval, BoundsRole.End, strict )
    new ma2.Range( getCFAxisName, startIndex, endIndex)
  }

  def getNormalizedCoordinate( cval: Double ): Double = coordAxis.getAxisType match {
    case nc2.constants.AxisType.Lon =>
      if( (cval<0.0) && ( coordAxis.getMinValue >= 0.0 ) ) cval + 360.0
      else if( (cval>180.0) && ( coordAxis.getMaxValue <= 180.0 ) ) cval - 360.0
      else cval
    case x => cval
  }

  def getGridCoordIndex(cval: Double, role: BoundsRole.Value, strict: Boolean = true): Int = {
    val coordAxis1D = CDSVariable.toCoordAxis1D( coordAxis )
    coordAxis1D.findCoordElement( getNormalizedCoordinate( cval ) ) match {
      case -1 =>
        if (role == BoundsRole.Start) {
          val grid_start = coordAxis1D.getCoordValue(0)
          logger.warn("Axis %s: ROI Start value %f outside of grid area, resetting to grid start: %f".format(coordAxis.getFullName, cval, grid_start))
          0
        } else {
          val end_index = coordAxis1D.getSize.toInt - 1
          val grid_end = coordAxis1D.getCoordValue(end_index)
          logger.warn("Axis %s: ROI Start value %s outside of grid area, resetting to grid end: %f".format(coordAxis.getFullName, cval, grid_end))
          end_index
        }
      case ival => ival
    }
  }
  def getNumericCoordValues( range: ma2.Range ): Array[Double] = {
    val coord_values = coordAxis.getCoordValues;
    if (coord_values.length == range.length) coord_values else (0 until range.length).map(iE => coord_values(range.element(iE))).toArray
  }

  def getGridIndexBounds( startval: Double, endval: Double, strict: Boolean = true): ma2.Range = {
    val startIndex = getGridCoordIndex(startval, BoundsRole.Start, strict)
    val endIndex = getGridCoordIndex(endval, BoundsRole.End, strict)
    new ma2.Range( getCFAxisName, startIndex, endIndex )
  }

  def getIndexBounds( startval: GenericNumber, endval: GenericNumber, strict: Boolean = false): ma2.Range = {
    val indexRange = if (coordAxis.getAxisType == nc2.constants.AxisType.Time) getTimeIndexBounds( startval.toString, endval.toString ) else getGridIndexBounds( startval, endval)
    assert(indexRange.last >= indexRange.first, "CDS2-CDSVariable: Coordinate bounds appear to be inverted: start = %s, end = %s".format(startval.toString, endval.toString))
    indexRange
  }
}

object GridSpec extends Loggable {
    def apply( variable: CDSVariable, roiOpt: Option[List[DomainAxis]] ): GridSpec = {
      val coordSpecs: IndexedSeq[Option[GridCoordSpec]] = for (idim <- variable.dims.indices; dim = variable.dims(idim); coord_axis_opt = variable.getCoordinateAxis(dim.getFullName)) yield coord_axis_opt match {
        case Some( coord_axis ) =>
          val domainAxisOpt: Option[DomainAxis] = roiOpt.flatMap(axes => axes.find(da => da.matches( coord_axis.getAxisType )))
          Some( new GridCoordSpec(idim, variable, coord_axis, domainAxisOpt) )
        case None => logger.warn( "Unrecognized coordinate axis: %s, axes = ( %s )".format( dim.getFullName, variable.getCoordinateAxesList.map( axis => axis.getFullName ).mkString(", ") )); None
      }
      new GridSpec( variable, coordSpecs.flatten )
    }
}

class  GridSpec( variable: CDSVariable, val axes: IndexedSeq[GridCoordSpec] ) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.cdm.GridCoordSpec")
  def getAxisSpec( dim_index: Int ): GridCoordSpec = axes(dim_index)
  def getAxisSpec( axis_type: AxisType ): Option[GridCoordSpec] = axes.find( axis => axis.getAxisType == axis_type )
  def getAxisSpec( domainAxis: DomainAxis ): Option[GridCoordSpec] = axes.find( axis => domainAxis.matches(axis.getAxisType ) )
  def getAxisSpec( cfAxisName: String ): Option[GridCoordSpec] = axes.find( axis => axis.getCFAxisName.toLowerCase.equals(cfAxisName.toLowerCase) )
  def getRank = axes.length
  def getBounds: Option[Array[Double]] = getAxisSpec("x").flatMap( xaxis => getAxisSpec("y").map( yaxis => Array( xaxis.bounds(0), yaxis.bounds(0), xaxis.bounds(1), yaxis.bounds(1) )) )
  def toXml: xml.Elem = <grid> { axes.map(_.toXml) } </grid>

  def getSection: ma2.Section = new ma2.Section( axes.map( _.getIndexRange ): _* )
  def addRangeNames( section: ma2.Section ): ma2.Section = new ma2.Section( getRanges( section )  )
  def getRanges( section: ma2.Section ): IndexedSeq[ma2.Range] = for( ir <- section.getRanges.indices; r0 = section.getRange(ir); axspec = getAxisSpec(ir) ) yield new ma2.Range( axspec.getCFAxisName, r0 )

  def getSubGrid( section: ma2.Section ): GridSpec = {
    assert( section.getRank == getRank, "Section with wrong rank for subgrid: %d vs %d ".format( section.getRank, getRank) )
    val subgrid_axes = section.getRanges.map( r => new DomainAxis( DomainAxis.fromCFAxisName(r.getName), r.first, r.last, "indices" ) )
    GridSpec( variable, Some(subgrid_axes.toList) )
  }

  def getSubSection( roi: List[DomainAxis] ): ma2.Section = {
    val ranges: IndexedSeq[ma2.Range] = for( gridCoordSpec <- axes ) yield {
      roi.find( _.matches( gridCoordSpec.getAxisType ) ) match {
        case Some( domainAxis ) => {
          val range = domainAxis.system match {
            case asys if asys.startsWith( "ind" ) => new ma2.Range(domainAxis.start.toInt, domainAxis.end.toInt)
            case asys if asys.startsWith( "val" ) =>
              val ibnds = gridCoordSpec.getIndexBounds( domainAxis.start, domainAxis.end )
              new ma2.Range(ibnds.first, ibnds.last )
            case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + domainAxis.system)
          }
          val irange = gridCoordSpec.getIndexRange.intersect( range )
          new ma2.Range( gridCoordSpec.getCFAxisName, irange.first, irange.last, 1)
        }
        case None => gridCoordSpec.getIndexRange
      }
    }
    new ma2.Section( ranges: _* )
  }
}


class TargetGrid( val variable: CDSVariable, roiOpt: Option[List[DomainAxis]] ) extends CDSVariable(variable.name, variable.dataset, variable.ncVariable ) {
  val grid = GridSpec( variable, roiOpt )
  def toBoundsString = roiOpt.map( _.map( _.toBoundsString ).mkString( "{ ", ", ", " }") ).getOrElse("")

  def addSectionMetadata( section: ma2.Section ): ma2.Section = grid.addRangeNames( section )

  def getSubGrid( section: ma2.Section ): TargetGrid = {
    assert( section.getRank == grid.getRank, "Section with wrong rank for subgrid: %d vs %d ".format( section.getRank, grid.getRank) )
    val subgrid_axes = section.getRanges.map( r => new DomainAxis( DomainAxis.fromCFAxisName(r.getName), r.first, r.last, "indices" ) )
    new TargetGrid( variable, Some(subgrid_axes.toList)  )
  }

//  def getAxisIndices( axisCFNames: String ): Array[Int] = for(cfName <- axisCFNames.toArray) yield grid.getAxisSpec(cfName.toString).map( _.index ).getOrElse(-1)

  def getPotentialAxisIndices( axisConf: List[OperationSpecs], flatten: Boolean = false ): AxisIndices = {
      val axis_ids = mutable.HashSet[Int]()
      for( opSpec <- axisConf ) {
        val axes = opSpec.getSpec("axes")
        val axis_chars: List[Char] = if( axes.contains(',') ) axes.split(",").map(_.head).toList else axes.toList
        axis_ids ++= axis_chars.map( cval => getAxisIndex( cval.toString ) )
      }
      val axisIdSet = if(flatten) axis_ids.toSet else  axis_ids.toSet
      new AxisIndices( axisIds=axisIdSet )
    }

  def getAxisIndices( axisConf: String ): AxisIndices = new AxisIndices( axisIds=axisConf.map( ch => getAxisIndex(ch.toString ) ).toSet )
  def getAxisIndex( cfAxisName: String, default_val: Int = -1 ): Int = grid.getAxisSpec( cfAxisName.toLowerCase ).map( gcs => gcs.index ).getOrElse( default_val )
  def getCFAxisName( dimension_index: Int ): String = grid.getAxisSpec( dimension_index ).getCFAxisName

  def getBounds( section: ma2.Section ): Option[Array[Double]] = {
    val xrangeOpt: Option[Array[Double]] = Option(section.find("x")).flatMap( (r: ma2.Range) => grid.getAxisSpec("x").map( (gs: GridCoordSpec) => gs.getBounds(r) ) )
    val yrangeOpt: Option[Array[Double]] = Option(section.find("y")).flatMap( (r: ma2.Range) => grid.getAxisSpec("y").map( (gs: GridCoordSpec) => gs.getBounds(r) ) )
    xrangeOpt.flatMap( xrange => yrangeOpt.map( yrange => Array( xrange(0), xrange(1), yrange(0), yrange(1) )) )
  }

//  def createFragmentSpec() = new DataFragmentSpec( variable.name, dataset.collection, Some(this) )

//  def loadPartition( data_variable: CDSVariable, fragmentSpec : DataFragmentSpec, maskOpt: Option[CDByteArray], axisConf: List[OperationSpecs] ): PartitionedFragment = {
//    val partition = fragmentSpec.partitions.head
//    val sp = new SectionPartitioner(fragmentSpec.roi, partition.nPart)
//    sp.getPartition(partition.partIndex, partition.axisIndex ) match {
//      case Some(partSection) =>
//        val array = data_variable.read(partSection)
//        val cdArray: CDFloatArray = CDFloatArray.factory(array, variable.missing )
//        new PartitionedFragment( cdArray, maskOpt, fragmentSpec )
//      case None =>
//        logger.warn("No fragment generated for partition index %s out of %d parts".format(partition.partIndex, partition.nPart))
//        new PartitionedFragment()
//    }
//  }

//  def loadRoi( data_variable: CDSVariable, fragmentSpec: DataFragmentSpec, maskOpt: Option[CDByteArray], dataAccessMode: DataAccessMode ): PartitionedFragment =
//    dataAccessMode match {
//      case DataAccessMode.Read => loadRoiDirect( data_variable, fragmentSpec, maskOpt )
//      case DataAccessMode.Cache =>  loadRoiViaCache( data_variable, fragmentSpec, maskOpt )
//      case DataAccessMode.MetaData =>  throw new Exception( "Attempt to load data in metadata operation")
//    }

  def loadRoiDirect( data_variable: CDSVariable, fragmentSpec: DataFragmentSpec, maskOpt: Option[CDByteArray] ): PartitionedFragment = {
    val array: ma2.Array = data_variable.read(fragmentSpec.roi)
    val cdArray: CDFloatArray = CDFloatArray.factory(array, data_variable.missing, maskOpt )
    val id = "a" + System.nanoTime.toHexString
    throw new IllegalAccessError( "Direct Read from NecCDF is not currently implemented")
 //   val part = new Partition( )
///    val partitions = new Partitions( id, fragmentSpec.getShape, Array(part) )
 //   new PartitionedFragment( partitions, maskOpt, fragmentSpec )
  }

  def loadFileDataIntoCache( data_variable: CDSVariable, fragmentSpec: DataFragmentSpec, maskOpt: Option[CDByteArray] ): PartitionedFragment = {
    logger.info( "loadRoiViaCache" )
    val cacheStream = new FileToCacheStream( data_variable.ncVariable, fragmentSpec.roi, maskOpt )
    val partitions: Partitions = cacheStream.cacheFloatData( data_variable.missing  )
    val newFragSpec = fragmentSpec.reshape( partitions.roi )
    val pfrag = new PartitionedFragment( partitions, maskOpt, newFragSpec )
    logger.info( "Persisting fragment %s with id %s".format( newFragSpec.getKey, partitions.id ) )
    FragmentPersistence.put( newFragSpec.getKey, partitions.id )
    pfrag
  }
}

class ServerContext( val dataLoader: DataLoader, private val configuration: Map[String,String] )  extends ScopeContext {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def getConfiguration = configuration
  def getVariable( collection: Collection, varname: String ): CDSVariable = dataLoader.getVariable( collection, varname )

  def getVariableData( fragSpec: DataFragmentSpec ): PartitionedFragment = {
    val fragFut = dataLoader.getExistingFragment( fragSpec ) match {
      case Some( fragFut ) => fragFut
      case None => cacheInputData( fragSpec )
    }
    Await.result( fragFut, Duration.Inf )
  }

  def inputs( inputSpecs: List[DataFragmentSpec] ): List[PartitionedFragment] =
    for( inputSpec <- inputSpecs ) yield getVariableData( inputSpec )

  def getAxisData( fragSpec: DataFragmentSpec, axis: Char ): Option[( Int, ma2.Array )] = {
    val variable: CDSVariable = dataLoader.getVariable(fragSpec.collection, fragSpec.varname)
    fragSpec.targetGridOpt.flatMap(targetGrid =>
      targetGrid.grid.getAxisSpec(axis.toString).map(axisSpec => {
        val range = fragSpec.roi.getRange(axisSpec.index)
        axisSpec.index -> axisSpec.coordAxis.read( List( range) )
      })
    )
  }

  def createTargetGrid( dataContainer: DataContainer, domainContainerOpt: Option[DomainContainer] ): TargetGrid = {
    val roiOpt: Option[List[DomainAxis]] = domainContainerOpt.map( domainContainer => domainContainer.axes )
    val t0 = System.nanoTime
    val variable: CDSVariable = dataLoader.getVariable( dataContainer.getSource.collection, dataContainer.getSource.name )
    val t1 = System.nanoTime
    val rv = new TargetGrid( variable, roiOpt )
    val t2 = System.nanoTime
    logger.info( " CreateTargetGridT: %.4f %.4f ".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
    rv
  }

  def getDataset(collection: Collection, varname: String ): CDSDataset = dataLoader.getDataset( collection, varname )



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

//  def getSubset( fragSpec: DataFragmentSpec, new_domain_container: DomainContainer, dataAccessMode: DataAccessMode = DataAccessMode.Read ): PartitionedFragment = {
//    val t0 = System.nanoTime
//    val baseFragment = dataLoader.getFragment( fragSpec, dataAccessMode )
//    val t1 = System.nanoTime
//    val variable = getVariable( fragSpec.collection, fragSpec.varname )
//    val targetGrid = fragSpec.targetGridOpt match { case Some(tg) => tg; case None => new TargetGrid( variable, Some(fragSpec.getAxes) ) }
//    val newFragmentSpec = targetGrid.createFragmentSpec( variable, targetGrid.grid.getSubSection(new_domain_container.axes),  new_domain_container.mask )
//    val rv = baseFragment.cutIntersection( newFragmentSpec.roi )
//    val t2 = System.nanoTime
//    logger.info( " GetSubsetT: %.4f %.4f".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
//    rv
//  }

  def createInputSpec( dataContainer: DataContainer, domain_container_opt: Option[DomainContainer], targetGrid: TargetGrid ): (String, DataFragmentSpec) = {
    val t0 = System.nanoTime
    val data_source: DataSource = dataContainer.getSource
    val variable: CDSVariable = dataLoader.getVariable(data_source.collection, data_source.name)
    val t1 = System.nanoTime
    val maskOpt: Option[String] = domain_container_opt.flatMap( domain_container => domain_container.mask )
    val section: ma2.Section = data_source.fragIdOpt match { case Some(fragId) => DataFragmentKey(fragId).getRoi; case None => targetGrid.grid.getSection }
    val domainSect: Option[ma2.Section] = domain_container_opt.map( domain_container => targetGrid.grid.getSubSection(domain_container.axes) )
    val fragSpec: DataFragmentSpec = new DataFragmentSpec( variable.name, variable.dataset.collection, data_source.fragIdOpt, Some(targetGrid), variable.ncVariable.getDimensionsString,
      variable.ncVariable.getUnitsString, variable.getAttributeValue( "long_name", variable.ncVariable.getFullName ), section, domainSect, variable.missing, maskOpt )
    val t2 = System.nanoTime
    val rv = dataContainer.uid -> fragSpec
    val t3 = System.nanoTime
    logger.info( " LoadVariableDataT: %.4f %.4f %.4f, T = %.4f ".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9, (t3-t2)/1.0E9, (t3-t0)/1.0E9 ) )
    rv
  }

  def cacheInputData( fragSpec: DataFragmentSpec ): Future[PartitionedFragment] = {
    logger.info( " ****>>>>>>>>>>>>> Cache Input Data: " + fragSpec.getKeyString )
    dataLoader.getExistingFragment(fragSpec) match {
      case Some(partFut) => partFut
      case None => dataLoader.cacheFragmentFuture(fragSpec)
    }
  }

  def cacheInputData( dataContainer: DataContainer, domain_container_opt: Option[DomainContainer], targetGrid: TargetGrid ): ( DataFragmentKey, Future[PartitionedFragment] ) = {
    val data_source: DataSource = dataContainer.getSource
    val variable: CDSVariable = dataLoader.getVariable(data_source.collection, data_source.name)
    val maskOpt: Option[String] = domain_container_opt.flatMap( domain_container => domain_container.mask )
    val section: ma2.Section = data_source.fragIdOpt match { case Some(fragId) => DataFragmentKey(fragId).getRoi; case None => targetGrid.grid.getSection }
    val domainSect: Option[ma2.Section] = domain_container_opt.map( domain_container => targetGrid.grid.getSubSection(domain_container.axes) )
    val fragSpec: DataFragmentSpec = new DataFragmentSpec( variable.name, variable.dataset.collection, data_source.fragIdOpt, Some(targetGrid), variable.ncVariable.getDimensionsString,
      variable.ncVariable.getUnitsString, variable.getAttributeValue( "long_name", variable.ncVariable.getFullName ), section, domainSect, variable.missing, maskOpt )
    dataLoader.getExistingFragment( fragSpec ) match {
      case Some( partFut ) => (fragSpec.getKey -> partFut )
      case None => (fragSpec.getKey -> dataLoader.cacheFragmentFuture(fragSpec) )
    }
  }

}



