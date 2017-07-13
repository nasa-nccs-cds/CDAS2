package nasa.nccs.esgf.process
import java.util.Formatter

import scala.xml
import nasa.nccs.caching._
import nasa.nccs.cdapi.cdm._
import ucar.nc2.dataset._
import java.util.Formatter

import nasa.nccs.cdapi.data._
import nasa.nccs.cdapi.tensors.{CDArray, CDByteArray, CDDoubleArray, CDFloatArray}
import nasa.nccs.cdas.engine.WorkflowNode
import nasa.nccs.cdas.engine.spark.{CDSparkContext, RangePartitioner, RecordKey, RecordKey$}
import nasa.nccs.cdas.kernels.{AxisIndices, KernelContext}
import nasa.nccs.cdas.utilities.appParameters
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.utilities.{Loggable, ProfilingTool, cdsutils}
import org.apache.spark.rdd.RDD
import ucar.nc2.Dimension
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
  def getExistingFragment( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode]  ): Option[Future[PartitionedFragment]]
  def cacheFragmentFuture( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode]  ): Future[PartitionedFragment];
  def deleteFragments( fragIds: Iterable[String] )
  def clearCache: Set[String]
}

trait ScopeContext {
  private lazy val __configuration__ = getConfiguration
  def getConfiguration: Map[String,String]
  def config( key: String, default: String ): String = __configuration__.getOrElse(key,default)
  def config( key: String ): Option[String] = __configuration__.get(key)
}

class RequestContext( val domains: Map[String,DomainContainer], val inputs: Map[String, Option[DataFragmentSpec]], request: TaskRequest, val profiler: ProfilingTool, private val configuration: Map[String,String] ) extends ScopeContext with Loggable {
  logger.info( "Creating RequestContext with inputs: " + inputs.keys.mkString(",") )
  def getConfiguration = configuration.map(identity)
  def getConf( key: String, default: String ) = configuration.getOrElse(key,default)
  def missing_variable(uid: String) = throw new Exception("Can't find Variable '%s' in uids: [ %s ]".format(uid, inputs.keySet.mkString(", ")))
  def getDataSources: Map[String, Option[DataFragmentSpec]] = inputs
  def getInputSpec( uid: String ): Option[DataFragmentSpec] = inputs.get( uid ).flatten
  def getInputSpec(): Option[DataFragmentSpec] = inputs.head._2
  def getCollection( serverContext: ServerContext, uid: String = "" ): Option[Collection] = inputs.get( uid ) match {
    case Some(optInputSpec) => optInputSpec map { inputSpec => inputSpec.getCollection }
    case None =>inputs.head._2 map { inputSpec => inputSpec.getCollection }
  }
  def getTargetGrids: Map[String,Option[TargetGrid]] = inputs.mapValues( _.flatMap( _.targetGridOpt ) )
  def getSection( serverContext: ServerContext, uid: String = "" ): Option[ma2.Section] = inputs.get( uid ) match {
    case Some(optInputSpec) => optInputSpec map { _.roi }
    case None =>inputs.head._2 map { _.roi }
  }
  def getTargetGridSpec( kernelContext: KernelContext ) : String = {
    if( kernelContext.crsOpt.getOrElse("").indexOf('~') > 0 ) { "gspec:" + kernelContext.crsOpt.get }
    else {
        val targetGrid: TargetGrid = getTargetGrid (kernelContext.grid.uid).getOrElse (throw new Exception ("Undefined Grid in domain partition for kernel " + kernelContext.operation.identifier) )
        targetGrid.getGridFile
    }
  }

  def getTimingReport(label: String): String = profiler.toString
  def logTimingReport(label: String): Unit = logger.info(getTimingReport(label))

  def getDomain(domain_id: String): DomainContainer = domains.get(domain_id) match {
    case Some(domain_container) => domain_container
    case None => throw new Exception("Undefined domain in ExecutionContext: " + domain_id)
  }
  def getTargetGrid( uid: String  ): Option[TargetGrid] = request.getTargetGrid( uid )
//  def getAxisIndices( axisConf: String ): AxisIndices = targetGrid.getAxisIndices( axisConf  )
}

class GridCoordSpec( val index: Int, val grid: CDGrid, val coordAxis: CoordinateAxis1D, val domainAxisOpt: Option[DomainAxis] )  extends Serializable with Loggable {
  private val _optRange: Option[ma2.Range] = getAxisRange( coordAxis, domainAxisOpt )
  private val _data = getCoordinateValues
  val bounds: Array[Double] = getAxisBounds( coordAxis, domainAxisOpt)
  def getData: Array[Double] = _data
  def getAxisType: AxisType = coordAxis.getAxisType

  def getCFAxisName: String = Option(coordAxis.getAxisType) match  {
    case Some( axisType ) => getAxisType.getCFAxisName
    case None => logger.warn( "Using %s for CFAxisName".format(coordAxis.getShortName) ); coordAxis.getShortName
  }
  def getAxisName: String = coordAxis.getFullName
  def getIndexRange: Option[ma2.Range] = _optRange
  def getLength: Int = _data.length
  def getStartValue: Double = bounds(0)
  def getEndValue: Double = bounds(1)
  def toXml: xml.Elem = <axis id={getAxisName} units={getUnits} cfName={getCFAxisName} type={getAxisType.toString} start={getStartValue.toString} end={getEndValue.toString} length={getLength.toString} > </axis>
  override def toString: String = "GridCoordSpec{id=%s units=%s cfName=%s type=%s start=%f end=%f length=%d}".format(getAxisName,getUnits,getCFAxisName,getAxisType.toString,getStartValue,getEndValue,getLength)
  def getMetadata: Map[String,String] = Map( "id"->getAxisName, "units"->getUnits, "name"->getCFAxisName, "type"->getAxisType.toString, "start"->getStartValue.toString, "end"->getEndValue.toString, "length"->getLength.toString )

  private def getAxisRange( coordAxis: CoordinateAxis, domainAxisOpt: Option[DomainAxis]): Option[ma2.Range] = {
    val axis_len = coordAxis.getShape(0)
    domainAxisOpt match {
      case Some( domainAxis ) =>  domainAxis.system match {
        case asys if asys.startsWith("ind") =>
          val dom_start = domainAxis.start.toInt
          if( dom_start < axis_len ) {
            Some( new ma2.Range(getCFAxisName, dom_start, math.min( domainAxis.end.toInt, axis_len-1 ), 1) )
          } else None
        case asys if asys.startsWith("val") => getIndexBounds( domainAxis.start, domainAxis.end )
        case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + domainAxis.system)
      }
      case None => Some( new ma2.Range( getCFAxisName, 0, axis_len-1, 1 ) )
    }
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

  def getBoundedCalDate(coordAxis1DTime: CoordinateAxis1DTime, caldate: CalendarDate, role: BoundsRole.Value, strict: Boolean = false): Option[CalendarDate] = {
    val date_range: CalendarDateRange = coordAxis1DTime.getCalendarDateRange
    if (!date_range.includes(caldate)) {
      if (strict) throw new IllegalStateException("CDS2-CDSVariable: Time value %s outside of time bounds %s".format(caldate.toString, date_range.toString))
      else {
        if (role == BoundsRole.Start) {
          if( caldate.isAfter( date_range.getEnd ) ) {
            logger.warn("Start time value %s > time range %s".format(caldate.toString, date_range.toString ) )
            None
          } else {
            val startDate: CalendarDate = date_range.getStart
            logger.warn("Time value %s outside of time bounds %s, resetting value to range start date %s".format(caldate.toString, date_range.toString, startDate.toString))
            Some(startDate)
          }
        } else {
          if( caldate.isBefore( date_range.getStart ) ) {
            logger.warn("End time value %s < time range %s".format(caldate.toString, date_range.toString ) )
            None
          } else {
            val endDate: CalendarDate = date_range.getEnd
            logger.warn("Time value %s outside of time bounds %s, resetting value to range end date %s".format(caldate.toString, date_range.toString, endDate.toString))
            Some(endDate)
          }
        }
      }
    } else {
//      logger.warn("Date %s IN time range %s".format(caldate.toString, date_range.toString ) )
      Some(caldate)
    }
  }

  def getCoordinateValues: Array[Double] = coordAxis.getAxisType match {
    case AxisType.Time =>
      val timeAxis: CoordinateAxis1DTime = getTimeAxis
      val timeCalValues: List[CalendarDate] = _optRange match {
        case None =>          timeAxis.getCalendarDates.toList
        case Some(range) =>   timeAxis.getCalendarDates.subList( range.first(), range.last() ).toList
      }
      timeCalValues.map( _.getMillis/1000.0 ).toArray
    case x =>_optRange match {
      case Some(range) => CDDoubleArray.factory( coordAxis.read(List(range)) ).getArrayData()
      case None =>        CDDoubleArray.factory( coordAxis.read() ).getArrayData()
    }
  }

  def getUnits: String =  coordAxis.getUnitsString // coordAxis.getAxisType match { case AxisType.Time => cdsutils.baseTimeUnits case x => coordAxis.getUnitsString }

  def getTimeAxis: CoordinateAxis1DTime = {
    val gridDS = NetcdfDatasetMgr.open(grid.gridFilePath )
    CoordinateAxis1DTime.factory( gridDS, coordAxis, new Formatter() )
  }

  def getTimeCoordIndex( tval: String, role: BoundsRole.Value, strict: Boolean = false): Option[Int] = {
    val coordAxis1DTime: CoordinateAxis1DTime = getTimeAxis
    val caldate: CalendarDate = cdsutils.dateTimeParser.parse(tval)
    getBoundedCalDate(coordAxis1DTime, caldate, role, strict).map( caldate_bounded => coordAxis1DTime.findTimeIndexFromCalendarDate(caldate_bounded) )
  }

  def getTimeIndexBounds( startval: String, endval: String, strict: Boolean = false): Option[ma2.Range] = {
//    logger.info( " getTimeIndexBounds: %s %s".format( startval, endval ) )
    getTimeCoordIndex( startval, BoundsRole.Start, strict).flatMap( startIndex =>
      getTimeCoordIndex( endval, BoundsRole.End, strict ).map( endIndex =>
        new ma2.Range( getCFAxisName, startIndex, endIndex) ) )
  }

  def getNormalizedCoordinate( cval: Double ): Double = coordAxis.getAxisType match {
    case nc2.constants.AxisType.Lon =>
      if( (cval<0.0) && ( coordAxis.getMinValue >= 0.0 ) ) cval + 360.0
      else if( (cval>180.0) && ( coordAxis.getMaxValue <= 180.0 ) ) cval - 360.0
      else cval
    case x => cval
  }

  def findCoordElement( coordAxis1D: CoordinateAxis1D, cval: Double ): Int = {   // work around ucar bug.
    if(coordAxis1D.isRegular) {
      val cvals =  coordAxis1D.getCoordValues
      assert( cvals.size > 0, "Empty coordinate axis: " + coordAxis1D.getFullName )
      if( cvals.size == 1 ) {
        if( cval == cvals(0) ) 0 else -1
      } else {
        val start = cvals.head
        val incr = cvals(1) - cvals(0)
        val index = math.round( ( cval - start ) / incr ).toInt
        if( (index >= cvals.size) || (index < 0) ) -1 else index
      }
    } else {
      coordAxis1D.findCoordElement( cval )
    }
  }

  def getGridCoordIndex(cval: Double, role: BoundsRole.Value, strict: Boolean = true): Option[Int] = {
    val coordAxis1D = CDSVariable.toCoordAxis1D( coordAxis )
    val ncval: Double = getNormalizedCoordinate( cval )
    findCoordElement( coordAxis1D, ncval ) match {
      case -1 =>
        val end_index = coordAxis1D.getSize.toInt - 1
        val grid_end = coordAxis1D.getCoordValue(end_index)
        val grid_start = coordAxis1D.getCoordValue(0)
        if (role == BoundsRole.Start) {
          if( ncval > grid_end ) {
            None
          } else {
            val grid_start = coordAxis1D.getCoordValue(0)
            logger.warn("Axis %s: ROI Start value %f outside of grid area, resetting to grid start: %f".format(coordAxis.getFullName, ncval, grid_start))
            Some(0)
          }
        } else {
          if( ncval <  grid_start ) {
            None
          } else {
            logger.warn("Axis %s: ROI Start value %s outside of grid area, resetting to grid end: %f".format(coordAxis.getFullName, ncval, grid_end))
            Some(end_index)
          }
        }
      case ival => Some(ival)
    }
  }
  def getNumericCoordValues( range: ma2.Range ): Array[Double] = {
    val coord_values = coordAxis.getCoordValues;
    if (coord_values.length == range.length) coord_values else (0 until range.length).map(iE => coord_values(range.element(iE))).toArray
  }

  def getGridIndexBounds( startval: Double, endval: Double, strict: Boolean = true): Option[ma2.Range] = {
    getGridCoordIndex(startval, BoundsRole.Start, strict).flatMap( startIndex =>
      getGridCoordIndex(endval, BoundsRole.End, strict).map( endIndex =>
        new ma2.Range( getCFAxisName, startIndex, endIndex ) ) )
  }

  def getIndexBounds( startval: GenericNumber, endval: GenericNumber, strict: Boolean = false): Option[ma2.Range] = {
    if (coordAxis.getAxisType == nc2.constants.AxisType.Time) {
      getTimeIndexBounds( startval.toString, endval.toString )
    } else getGridIndexBounds( startval, endval )
//    assert(indexRange.last >= indexRange.first, "CDS2-CDSVariable: Coordinate bounds appear to be inverted: start = %s, end = %s".format(startval.toString, endval.toString))
//    indexRange
  }
}

object GridSection extends Loggable {
  def apply( variable: CDSVariable, roiOpt: Option[List[DomainAxis]] ): GridSection = {
    val grid = variable.collection.grid
    val axes = variable.getCoordinateAxesList
    val coordSpecs: IndexedSeq[Option[GridCoordSpec]] = for (idim <- variable.dims.indices; dim = variable.dims(idim); coord_axis_opt = variable.getCoordinateAxis(dim)) yield coord_axis_opt match {
      case Some( coord_axis ) =>
        val domainAxisOpt: Option[DomainAxis] = roiOpt.flatMap(axes => axes.find(da => da.matches( coord_axis.getAxisType )))
        Some( new GridCoordSpec(idim, grid, coord_axis, domainAxisOpt) )
      case None =>
        logger.warn( "Unrecognized coordinate axis: %s, axes = ( %s )".format( dim, grid.getCoordinateAxes.map( axis => axis.getFullName ).mkString(", ") )); None
    }
    new GridSection( grid, coordSpecs.flatten )
  }

  def apply( variable: CDSVariable, section: ma2.Section ): GridSection = {
    val grid_axes = section.getRanges.map( r => new DomainAxis( DomainAxis.fromCFAxisName(r.getName), r.first, r.last, "indices" ) )
    GridSection( variable, Some( grid_axes.toList) )
  }
}

class  GridSection( val grid: CDGrid, val axes: IndexedSeq[GridCoordSpec] ) extends Serializable with Loggable {
  def getAxisSpec( dim_index: Int ): GridCoordSpec = axes(dim_index)
  def getAxisSpec( axis_type: AxisType ): Option[GridCoordSpec] = axes.find( axis => axis.getAxisType == axis_type )
  def getAxisSpec( domainAxis: DomainAxis ): Option[GridCoordSpec] = axes.find( axis => domainAxis.matches(axis.getAxisType ) )
  def getAxisSpec( cfAxisName: String ): Option[GridCoordSpec] = {
    val anames = axes map ( _.getCFAxisName )
    axes.find( axis => axis.getCFAxisName.toLowerCase.equals(cfAxisName.toLowerCase) )
  }
  def getRank = axes.length
  def getBounds: Option[Array[Double]] = getAxisSpec("x").flatMap( xaxis => getAxisSpec("y").map( yaxis => Array( xaxis.bounds(0), yaxis.bounds(0), xaxis.bounds(1), yaxis.bounds(1) )) )
  def toXml: xml.Elem = <grid> { axes.map(_.toXml) } </grid>
  def getGridSpec: String  = grid.getGridSpec
  def getGridFile: String  = grid.getGridFile
  def getTimeCoordinateAxis: Option[CoordinateAxis1DTime] = grid.getTimeCoordinateAxis
//  def getCalendarDate ( idx: Int ): CalendarDate = grid.getTimeCoordinateAxis match { case Some(axis) => axis.getCalendarDate(idx); case None => throw new Exception( "Can't get the time axis for grid " + grid.name ) }

  def getCalendarDate ( idx: Int ): CalendarDate = grid.getTimeCoordinateAxis match {
    case Some(axis) =>
      val testdate = axis.getCalendarDate(0);
      try { axis.getCalendarDate(idx); }
      catch {
        case err: IndexOutOfBoundsException =>
          throw err;
      }
    case None =>
      throw new Exception("Can't get time axis for grid " + grid.name)
  }

  def getTimeRange: RecordKey = grid.getTimeCoordinateAxis match {
    case Some(axis) =>
      val ( t0, t1 ) = ( axis.getCalendarDate(0), axis.getCalendarDate( axis.getSize.toInt-1 ) )
      RecordKey( t0.getMillis/1000, t1.getMillis/1000, 0, axis.getSize.toInt )
    case None =>
      throw new Exception("Can't get time axis for grid " + grid.name)
  }

  def getNextCalendarDate ( axis: CoordinateAxis1DTime, idx: Int ): CalendarDate = {
    if( (idx+1) < axis.getSize ) { axis.getCalendarDate(idx+1) }
    else {
      val (d0, d1) = (axis.getCalendarDate(idx - 1).getMillis, axis.getCalendarDate(idx).getMillis)
      CalendarDate.of(d1 + (d1 - d0))
    }
  }

  def getSection: Option[ma2.Section] = {
    val ranges = for( axis <- axes ) yield axis.getIndexRange match { case Some( range ) => range; case None => return None }
    Some( new ma2.Section( ranges: _* ) )
  }
  def addRangeNames( section: ma2.Section ): ma2.Section = new ma2.Section( getRanges( section )  )
  def getRanges( section: ma2.Section ): IndexedSeq[ma2.Range] = for( ir <- section.getRanges.indices; r0 = section.getRange(ir); axspec = getAxisSpec(ir) ) yield new ma2.Range( axspec.getCFAxisName, r0 )

  def getSubSection( roi: List[DomainAxis] ): Option[ma2.Section] = {
//    val coordSystem = grid.coordSystems.head
//    val roi_axes = for( dim: Dimension  <- coordSystem.getDomain ) roi.find( da => da.getCoordAxisName.matches(dim.getShortName) ) match {
//      case Some( roi_dim ) => roi_dim
//      case None => dim
//    }
    val ranges: IndexedSeq[ma2.Range] = for( gridCoordSpec <- axes ) yield {
      roi.find( _.matches( gridCoordSpec.getAxisType ) ) match {
        case Some( domainAxis ) => {
          val range = domainAxis.system match {
            case asys if asys.startsWith( "ind" ) => new ma2.Range(domainAxis.start.toInt, domainAxis.end.toInt)
            case asys if asys.startsWith( "val" ) =>
 //             logger.info( "  %s getIndexBounds from %s".format( gridCoordSpec.toString, domainAxis.toString ) )
              gridCoordSpec.getIndexBounds( domainAxis.start, domainAxis.end ) match {
                case Some(ibnds) => new ma2.Range (ibnds.first, ibnds.last)
                case None => return None
              }
            case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + domainAxis.system)
          }
//          val irange = gridCoordSpec.getIndexRange.intersect( range )
          new ma2.Range( gridCoordSpec.getCFAxisName, range.first, range.last, 1)
        }
        case None => gridCoordSpec.getIndexRange match {
          case Some( range ) => range
          case None => return None
        }
      }
    }
    Some( new ma2.Section( ranges: _* ) )
  }
}

object CDSection {
  def apply( section: ma2.Section ): CDSection = new CDSection( section.getOrigin, section.getShape )
  def relative( section: ma2.Section ): CDSection = new CDSection( Array.fill(section.getRank)(0), section.getShape )
  def empty( rank: Int ): CDSection = new CDSection( Array.fill(rank)(0), Array.fill(rank)(0) )
  def serialize( section: ma2.Section): String = section.getRanges map ( r => r.getName + "," + r.first.toString + "," + r.last.toString ) mkString("+")
  def deserialize( section: String ): ma2.Section = new ma2.Section( section.split('+').map( rspec => { val sspec = rspec.split(','); new ma2.Range(sspec(0).trim,sspec(1).toInt,sspec(2).toInt) } ):_* )
  def merge( sect0: String, sect1: String ): String = CDSection.serialize( CDSection.deserialize(sect0).union( CDSection.deserialize(sect1) ) )
  def getRange( section: ma2.Section, axis: String ): Option[ma2.Range] = section.getRanges.find( _.getName == axis )
}
class CDSection( origin: Array[Int], shape: Array[Int] ) extends Serializable {
  def toSection: ma2.Section = new ma2.Section( origin, shape )
  def toSection(offset: Array[Int]): ma2.Section = new ma2.Section( origin, shape ).shiftOrigin( new ma2.Section( offset, shape ) )
  def getRange( axis_index: Int ) = toSection.getRange(axis_index)
  def getShape = toSection.getShape
  def getOrigin = toSection.getOrigin
  override def toString() = "Section[%s:%s]".format( origin.mkString(","), shape.mkString(","))
  def merge( cdsect: CDSection ): CDSection = CDSection( cdsect.toSection.union( toSection ) )
}

object GridContext extends Loggable {
  def apply( uid: String, targetGrid: TargetGrid ) : GridContext = {
    val axisMap: Map[Char,Option[( Int, HeapDblArray )]] = Map( List( 'z', 'y', 'x' ).map( axis => axis -> targetGrid.getSpatialAxisData(axis) ):_* )
    val timeAxis: Option[ HeapLongArray ] = targetGrid.getTimeAxisData
    val cfAxisNames: Array[String] = ( 0 until targetGrid.getRank ).map( dim_index => targetGrid.getCFAxisName( dim_index ) ).toArray
    val axisIndexMap: Map[String,Int] = Map( cfAxisNames.map( cfAxisName => cfAxisName.toLowerCase -> targetGrid.getAxisIndex(cfAxisName) ):_* )
    new GridContext(uid,axisMap,timeAxis,cfAxisNames,axisIndexMap)
  }
}

class GridContext(val uid: String, val axisMap: Map[Char,Option[( Int, HeapDblArray )]], val timeAxis: Option[ HeapLongArray ], val cfAxisNames: Array[String], val axisIndexMap: Map[String,Int] ) extends Serializable {
  def getAxisIndices( axisConf: String ): AxisIndices = new AxisIndices( axisIds=axisConf.map( ch => getAxisIndex( ch.toString.toLowerCase ) ).toSet )
  def getAxisIndex( cfAxisName: String, default_val: Int = -1 ): Int = axisIndexMap.getOrElse( cfAxisName, default_val )
  def getCFAxisName( dimension_index: Int ): String = cfAxisNames(dimension_index)
  def getTimeAxisData: Option[( HeapLongArray )] = timeAxis
  def getSpatialAxisData( axis: Char ): Option[( Int, HeapDblArray )] = axisMap.getOrElse( axis, None )
  def getSpatialAxisData( axis: Char, section: CDSection ): Option[( Int, ma2.Array )] = axisMap.getOrElse( axis, None ).map {
    case ( axis_index, array ) => ( axis_index, array.toUcarDoubleArray.section( List( section.getRange(axis_index) ) ) )
  }
  def getSpatialAxisData( axis: Char, section: Option[CDSection] ): Option[( Int, ma2.Array )] = section match {
    case Some(section) => getSpatialAxisData( axis, section );
    case None => getSpatialAxisData( axis ).map { case ( index, array ) => ( index, array.toUcarDoubleArray ) }
  }
}

class TargetGrid( variable: CDSVariable, roiOpt: Option[List[DomainAxis]]=None ) extends CDSVariable( variable.name, variable.collection ) {
  val grid = GridSection( variable, roiOpt )
  def toBoundsString = roiOpt.map( _.map( _.toBoundsString ).mkString( "{ ", ", ", " }") ).getOrElse("")
  def getRank = grid.getRank
  def getGridSpec: String  = grid.getGridSpec
  def getGridFile: String  = grid.getGridFile
  def getTimeCoordinateAxis: Option[CoordinateAxis1DTime] = grid.getTimeCoordinateAxis
  def getTimeUnits: String  = grid.getTimeCoordinateAxis match { case Some(timeAxis) => timeAxis.getUnitsString; case None => "" }
  def getCalendarDate ( idx: Int ): CalendarDate = grid.getCalendarDate(idx)

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

  def getAxisData( axis: Char, section: ma2.Section ): Option[( Int, ma2.Array )] = {
    grid.getAxisSpec(axis.toString).map(axisSpec => {
      val range = section.getRange(axisSpec.index)
      axisSpec.index -> axisSpec.coordAxis.read( List( range) )
    })
  }

  def getAxisData( axis: Char ): Option[( Int, ma2.Array )] = {
    grid.getAxisSpec(axis.toString).map(axisSpec => {
      axisSpec.index -> axisSpec.coordAxis.read()
    })
  }
  def getSpatialAxisData( axis: Char ): Option[( Int, HeapDblArray )] = {
    grid.getAxisSpec(axis.toString) match {
      case Some(axisSpec) => axisSpec.coordAxis.getAxisType match {
        case AxisType.Time => None
        case x => Some(axisSpec.index -> HeapDblArray(axisSpec.coordAxis.read(), Array(0), axisSpec.getMetadata, variable.missing))
      }
      case None => None
    }
  }

  def getTimeAxisData: Option[ HeapLongArray ] = {
    grid.getAxisSpec("t") match {
      case Some(axisSpec) => axisSpec.coordAxis.getAxisType match {
        case AxisType.Time => Some( HeapLongArray( axisSpec.coordAxis.read(), Array(0), axisSpec.getMetadata, variable.missing ) )
        case x => None
      }
      case None => None
    }
  }

  def getBounds( section: ma2.Section ): Option[Array[Double]] = {
    val xrangeOpt: Option[Array[Double]] = Option( section.find("X") ) flatMap ( (r: ma2.Range) => grid.getAxisSpec("X").map( (gs: GridCoordSpec) => gs.getBounds(r) ) )
    val yrangeOpt: Option[Array[Double]] = Option( section.find("Y") ) flatMap ( (r: ma2.Range) => grid.getAxisSpec("y").map(( gs: GridCoordSpec) => gs.getBounds(r) ) )
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

//  def loadRoiDirect( data_variable: CDSVariable, fragmentSpec: DataFragmentSpec, maskOpt: Option[CDByteArray] ): PartitionedFragment = {
//    val array: ma2.Array = data_variable.read(fragmentSpec.roi)
//    val cdArray: CDFloatArray = CDFloatArray.factory(array, data_variable.missing, maskOpt )
//    val id = "a" + System.nanoTime.toHexString
//    throw new IllegalAccessError( "Direct Read from NecCDF is not currently implemented" )
// //   val part = new Partition( )
/////    val partitions = new Partitions( id, fragmentSpec.getShape, Array(part) )
// //   new PartitionedFragment( partitions, maskOpt, fragmentSpec )
//  }

  def loadFileDataIntoCache( fragmentSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode], maskOpt: Option[CDByteArray] ): PartitionedFragment = {
    logger.info( "loadRoiViaCache" )
    val cacheStream = new FileToCacheStream( fragmentSpec, partsConfig, workflowNodeOpt, maskOpt )
    val partitions: CachePartitions = cacheStream.cacheFloatData
    val newFragSpec = fragmentSpec.reshape( partitions.roi )
    val pfrag = new PartitionedFragment( partitions, maskOpt, newFragSpec )
    logger.info( "Persisting fragment %s with id %s".format( newFragSpec.getKey, partitions.id ) )
    FragmentPersistence.put( newFragSpec.getKey, partitions.id )
    pfrag
  }
}

class ServerContext( val dataLoader: DataLoader, val spark: CDSparkContext )  extends ScopeContext with Serializable with Loggable  {
  def getVariable( collection: Collection, varname: String ): CDSVariable = collection.getVariable(varname)
  def getConfiguration: Map[String,String] = appParameters.getParameterMap

  def getOperationInput( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNode: WorkflowNode ): OperationInput = {
    dataLoader.getExistingFragment( fragSpec, partsConfig, Some(workflowNode) ) match {
      case Some( fragFut ) => Await.result( fragFut, Duration.Inf )
      case None =>
        if(fragSpec.autoCache) {
          val fragFut = cacheInputData( fragSpec, partsConfig, Some(workflowNode) )
          Await.result( fragFut, Duration.Inf )
        }
        else { new CDASDirectDataInput( fragSpec, partsConfig, workflowNode ) }
    }
  }

  def getAxisData( fragSpec: DataFragmentSpec, axis: Char ): Option[( Int, ma2.Array )] = {
    val variable: CDSVariable = fragSpec.getVariable
    fragSpec.targetGridOpt.flatMap(targetGrid =>
      targetGrid.grid.getAxisSpec(axis.toString).map(axisSpec => {
        val range = fragSpec.roi.getRange(axisSpec.index)
        axisSpec.index -> axisSpec.coordAxis.read( List( range) )
      })
    )
  }

//  def createTargetGrid( dataContainer: DataContainer, domainContainerOpt: Option[DomainContainer] ): TargetGrid = {
//    val roiOpt: Option[List[DomainAxis]] = domainContainerOpt.map( domainContainer => domainContainer.axes )
//    val t0 = System.nanoTime
//    lazy val variable: CDSVariable = dataContainer.getVariable
//    val t1 = System.nanoTime
//    val rv = new TargetGrid( variable, roiOpt )
//    val t2 = System.nanoTime
//    logger.info( " CreateTargetGridT: %.4f %.4f ".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
//    rv
//  }

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

  def createInputSpec( dataContainer: DataContainer, domain_container_opt: Option[DomainContainer],  request: TaskRequest ): (String, Option[DataFragmentSpec]) = {
    val t0 = System.nanoTime
    val data_source: DataSource = dataContainer.getSource
    logger.info( s"Data Source, fragIdOpt: ${data_source.fragIdOpt.toString}" )
    val variable: CDSVariable = dataContainer.getVariable
    val targetGrid = request.getTargetGrid( dataContainer )
    val t1 = System.nanoTime
    val maskOpt: Option[String] = domain_container_opt.flatMap( domain_container => domain_container.mask )
    val fragRoiOpt = data_source.fragIdOpt.map( fragId => DataFragmentKey(fragId).getRoi )
    val domain_mdata = domain_container_opt.map( _.metadata ).getOrElse(Map.empty)
    val optSection: Option[ma2.Section] = fragRoiOpt match { case Some(roi) => Some(roi); case None => targetGrid.grid.getSection }
    val optDomainSect: Option[ma2.Section] = domain_container_opt.flatMap( domain_container => targetGrid.grid.getSubSection(domain_container.axes) )
    val fragSpec: Option[DataFragmentSpec] = optSection map { section =>
      new DataFragmentSpec( dataContainer.uid, variable.name, variable.collection, data_source.fragIdOpt, Some(targetGrid), variable.dims.mkString(","),
        variable.units, variable.getAttributeValue("long_name", variable.fullname), section, optDomainSect, domain_mdata, variable.missing, variable.getAttributeValue("numDataFiles", "1").toInt, maskOpt, data_source.autoCache )
    }
    val t2 = System.nanoTime
    val rv = dataContainer.uid -> fragSpec
    val t3 = System.nanoTime
    logger.info( " LoadVariableDataT: section=%s, domainSect=%s, fragId=%s, fragRoi=%s, %.4f %.4f %.4f, T = %.4f ".format(
      optSection.getOrElse("null").toString, optDomainSect.getOrElse("null").toString, data_source.fragIdOpt.getOrElse("null").toString, fragRoiOpt.getOrElse("null").toString, (t1-t0)/1.0E9, (t2-t1)/1.0E9, (t3-t2)/1.0E9, (t3-t0)/1.0E9 ) )
    rv
  }

  def cacheInputData( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode] ): Future[PartitionedFragment] = {
    logger.info( " ****>>>>>>>>>>>>> Cache Input Data: " + fragSpec.getKeyString )
    dataLoader.getExistingFragment( fragSpec, partsConfig, workflowNodeOpt ) match {
      case Some(partFut) => partFut
      case None => dataLoader.cacheFragmentFuture( fragSpec, partsConfig, workflowNodeOpt )
    }
  }

  def deleteFragments( fragIds: Iterable[String] ) = {
    dataLoader.deleteFragments( fragIds )
  }

  def clearCache: Set[String] = dataLoader.clearCache

  def cacheInputData( dataContainer: DataContainer, partsConfig: Map[String,String], domain_container_opt: Option[DomainContainer], targetGrid: TargetGrid, workflowNodeOpt: Option[WorkflowNode]  ): Option[( DataFragmentKey, Future[PartitionedFragment] )] = {
    val data_source: DataSource = dataContainer.getSource
    logger.info( "cacheInputData"  )
    val variable: CDSVariable = dataContainer.getVariable
    val maskOpt: Option[String] = domain_container_opt.flatMap( domain_container => domain_container.mask )
    val optSection: Option[ma2.Section] = data_source.fragIdOpt match {
      case Some(fragId) => Some(DataFragmentKey(fragId).getRoi);
      case None => targetGrid.grid.getSection
    }
    val optDomainSect: Option[ma2.Section] = domain_container_opt.flatMap( domain_container => targetGrid.grid.getSubSection(domain_container.axes) )
    val domain_mdata = domain_container_opt.map( _.metadata ).getOrElse(Map.empty)
    if( optSection == None ) logger.warn( "Attempt to cache empty segment-> No caching will occur: " + dataContainer.toString )
    optSection map { section =>
      val fragSpec = new DataFragmentSpec( dataContainer.uid, variable.name, variable.collection, data_source.fragIdOpt, Some(targetGrid), variable.dims.mkString(","),
      variable.units, variable.getAttributeValue("long_name", variable.fullname), section, optDomainSect, domain_mdata, variable.missing, variable.getAttributeValue("numDataFiles", "1").toInt, maskOpt, data_source.autoCache )
      logger.info( "cache fragSpec: " + fragSpec.getKey.toString )
      dataLoader.getExistingFragment(fragSpec, partsConfig, workflowNodeOpt) match {
        case Some(partFut) =>
          logger.info( "Found existing cached fragment to match: " + fragSpec.getKey.toString )
          (fragSpec.getKey -> partFut)
        case None =>
          logger.info( "Creating new cached fragment: " + fragSpec.getKey.toString )
          (fragSpec.getKey -> dataLoader.cacheFragmentFuture(fragSpec, partsConfig, workflowNodeOpt))
      }
    }
  }
}


