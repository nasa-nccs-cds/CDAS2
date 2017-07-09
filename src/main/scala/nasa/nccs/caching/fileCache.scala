package nasa.nccs.caching

import java.io._
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, PathMatcher, Paths}
import java.nio.{ByteBuffer, FloatBuffer, MappedByteBuffer}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Comparator}

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EntryWeigher, EvictionListener}
import nasa.nccs.caching.CDASPartitioner.{partitionSize, recordSize}
import nasa.nccs.cdas.utilities.{GeoTools, appParameters, runtime}
import nasa.nccs.cdapi.cdm.{PartitionedFragment, _}
import nasa.nccs.cdapi.data.RDDRecord
import nasa.nccs.cdapi.tensors.{CDByteArray, CDFloatArray}
import nasa.nccs.cdas.engine.WorkflowNode
import nasa.nccs.cdas.engine.spark.RecordKey
import nasa.nccs.cdas.kernels.TransientFragment
import nasa.nccs.cdas.loaders.Masks
import nasa.nccs.esgf.process.{DataFragmentKey, _}
import nasa.nccs.esgf.wps.cds2ServiceProvider
import nasa.nccs.utilities.{Loggable, cdsutils}
import org.apache.commons.io.{FileUtils, IOUtils}
import ucar.ma2.Range
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.nc2.time.CalendarPeriod.Field.{Month, Year}
import ucar.{ma2, nc2}

import scala.xml
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

object MaskKey {
  def apply(bounds: Array[Double],
            mask_shape: Array[Int],
            spatial_axis_indices: Array[Int]): MaskKey = {
    new MaskKey(bounds,
                Array(mask_shape(spatial_axis_indices(0)),
                      mask_shape(spatial_axis_indices(1))))
  }
}
class MaskKey(bounds: Array[Double], dimensions: Array[Int]) {}

class CacheChunk(val offset: Int,
                 val elemSize: Int,
                 val shape: Array[Int],
                 val buffer: ByteBuffer) {
  def size: Int = shape.product
  def data: Array[Byte] = buffer.array
  def byteSize = shape.product * elemSize
  def byteOffset = offset * elemSize
}

object BatchSpec extends Loggable {
  lazy val serverContext = cds2ServiceProvider.cds2ExecutionManager.serverContext
  lazy val nCores = appParameters( "parts.per.node", "1" ).toInt
  lazy val nNodes = appParameters( "num.cluster.nodes", "1" ).toInt
  lazy val nParts = nCores * nNodes
  def apply( index: Int ): BatchSpec = { new BatchSpec( index*nParts, nParts ) }
}

case class BatchSpec( iStartPart: Int, nParts: Int ) {
  def included( part_index: Int ): Boolean = (part_index >= iStartPart ) && ( nParts > (part_index-iStartPart)  )
  override def toString(): String = "Batch{start: %d, size: %d}".format( iStartPart, nParts )
}

class CachePartitions( val id: String, private val _section: ma2.Section, val parts: Array[CachePartition]) extends Loggable {
  private val baseShape = _section.getShape
  def getShape = baseShape
  def getPart(partId: Int): CachePartition = parts(partId)
  def getPartData(partId: Int, missing_value: Float): CDFloatArray = parts(partId).data(missing_value)
  def roi: ma2.Section = new ma2.Section(_section.getRanges)
  def delete = parts.map(_.delete)

  def getBatch( batchIndex: Int ): Array[CachePartition] = {
    val batch = BatchSpec(batchIndex)
    val rv = parts.filter( p => batch.included(p.index) )
    logger.info( "Get [%d]%s, selection size = %d".format( batchIndex, batch.toString, rv.length ) )
    rv
  }
}

class Partitions( private val _section: ma2.Section, val parts: Array[Partition]) {
  private val baseShape = _section.getShape
  def getShape = baseShape
  def getPart(partId: Int): Partition = parts(partId)
  def roi: ma2.Section = new ma2.Section(_section.getRanges)

  def getBatch( batchIndex: Int ): Array[Partition] = {
    val batch = BatchSpec(batchIndex)
    parts.filter( p => batch.included(p.index) )
  }
}

object CachePartition {
  def apply(index: Int, path: String, dimIndex: Int, startIndex: Int, partSize: Int, recordSize: Int, sliceMemorySize: Long, origin: Array[Int], fragShape: Array[Int]): CachePartition = {
    val partShape = getPartitionShape(partSize, fragShape)
    new CachePartition(index, path, dimIndex, startIndex, partSize, recordSize, sliceMemorySize, origin, partShape)
  }
  def apply(path: String, partition: RegularPartition ): CachePartition = {
    val partShape = getPartitionShape( partition.partSize, partition.shape )
    new CachePartition( partition.index, path, partition.dimIndex, partition.startIndex, partition.partSize, partition.recordSize, partition.sliceMemorySize, partition.origin, partShape )
  }
  def getPartitionShape(partSize: Int, fragShape: Array[Int]): Array[Int] = {
    var shape = fragShape.clone(); shape(0) = partSize; shape
  }
}

class CachePartition( index: Int, val path: String, dimIndex: Int, startIndex: Int, partSize: Int, recordSize: Int, sliceMemorySize: Long, origin: Array[Int], shape: Array[Int]) extends RegularPartition(index, dimIndex, startIndex, partSize, recordSize, sliceMemorySize, origin, shape) {

  def data(missing_value: Float): CDFloatArray = {
    val file = new RandomAccessFile(path, "r")
    val channel: FileChannel = file.getChannel()
    logger.debug(s" *** Mapping channel for Partition-$index with partSize=$partSize startIndex=$startIndex, recordSize=$recordSize, sliceMemorySize=$sliceMemorySize, shape=(%s), path=%s".format( shape.mkString(","), path ))
//    if( index == 0 ) { logger.debug( "\n    " + Thread.currentThread().getStackTrace.mkString("\n    ")) }
    val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, partSize * sliceMemorySize)
    channel.close(); file.close()
    runtime.printMemoryUsage(logger)
    new CDFloatArray(shape, buffer.asFloatBuffer, missing_value)
  }
  def delete = { FileUtils.deleteQuietly(new File(path)) }

  def dataSection(section: CDSection, missing_value: Float): CDFloatArray =
    try {
      val partData = data(missing_value)
      logger.info( " &&& PartSection: section = {s:%s||o:%s}, partOrigin = {%s}".format( section.getShape.mkString(","), section.getOrigin.mkString(","), partitionOrigin.mkString(",") ))
      val partSection = section.toSection( partitionOrigin )
      partData.section( partSection )
    } catch {
      case ex: AssertionError =>
        logger.error(" Error in dataSection, section origin = %s, shape = %s".format( section.getOrigin.mkString(","), section.getShape.mkString(",")) )
        throw ex
    }
}

abstract class Partition(val index: Int, val dimIndex: Int, val startIndex: Int, val partSize: Int, val sliceMemorySize: Long, val origin: Array[Int], val shape: Array[Int]) extends Loggable with Serializable {
  val partitionOrigin: Array[Int] = origin.zipWithIndex map { case (value, ival) => if( ival== 0 ) value + startIndex else value }
  val endIndex: Int = startIndex + partSize - 1

  def recordSection( section: ma2.Section, iRecord: Int ): ma2.Section = {
    val rec_range = recordRange(iRecord)
    val rv = new ma2.Section(section.getRanges).replaceRange(dimIndex, rec_range ).intersect(section)
    logger.info( " *** RecordSection[%d]: dim=%d, range=[ %d, %d ]: %s -> %s ".format(iRecord,dimIndex,rec_range.first,rec_range.last, section.toString, rv.toString ) )
    rv
  }
  override def toString() = s"Part[$index]{ start=$startIndex, size=$partSize, origin=(${origin.mkString(",")}), shape=(${shape.mkString(",")}) ]"
  def partSection(section: ma2.Section): ma2.Section = {
    new ma2.Section(section.getRanges).replaceRange(dimIndex, partRange)
  }
  def partRange: ma2.Range = { new ma2.Range( origin(0)+startIndex, origin(0)+endIndex) }

  def recordRange(iRecord: Int): ma2.Range
  def nRecords: Int
  def recordStartIndex(iRecord: Int): Int

  def getPartitionRecordKey(grid: TargetGrid ): RecordKey = {
    val start = origin(0)+startIndex
    val startDate = grid.getCalendarDate(start)
    val startDateStr = startDate.toString
    val startTime = startDate.getMillis/1000
    val end = Math.min( start+partSize, grid.shape(0)-1 )
    val endDate = grid.getCalendarDate(end)
    val endDateStr = endDate.toString
    val endTime =  grid.getCalendarDate(end).getMillis/1000
    RecordKey( startTime, endTime, startIndex, partSize )
  }

  def getRecordKey( iRecord: Int, grid: TargetGrid ): RecordKey

  def getRelativeSection(global_section: ma2.Section): ma2.Section = {
    val relative_ranges = for (ir <- global_section.getRanges.indices; r = global_section.getRange(ir)) yield {
      if (ir == dimIndex) { r.shiftOrigin(startIndex) } else r
    }
    new ma2.Section(relative_ranges)
  }
}


object RegularPartition {
  def apply(index: Int, dimIndex: Int, startIndex: Int, partSize: Int, recordSize: Int, sliceMemorySize: Long, origin: Array[Int], fragShape: Array[Int]): RegularPartition = {
    val partShape = getPartitionShape(partSize, fragShape)
    new RegularPartition( index, dimIndex, startIndex, partSize, recordSize, sliceMemorySize, origin, partShape )
  }
  def getPartitionShape(partSize: Int, fragShape: Array[Int]): Array[Int] = {
    var shape = fragShape.clone(); shape(0) = partSize; shape
  }

}
class RegularPartition( index: Int,  dimIndex: Int,  startIndex: Int,  partSize: Int,  val recordSize: Int,  sliceMemorySize: Long,  origin: Array[Int],  shape: Array[Int]) extends Partition(index, dimIndex, startIndex, partSize, sliceMemorySize, origin, shape) {

  override val toString: String = s"Part[$index]{dim:$dimIndex start:$startIndex partSize:$partSize recordSize:$recordSize sliceMemorySize:$sliceMemorySize origin:${origin.mkString(",")} shape:${shape.mkString(",")})"
  override def nRecords: Int = math.ceil(partSize / recordSize.toDouble).toInt

  override def recordRange(iRecord: Int): ma2.Range = {
    val start = recordStartIndex(iRecord);
    new ma2.Range(start, origin(0)+Math.min(start + recordSize - 1, endIndex))
  }

  override def getRecordKey( iRecord: Int, grid: TargetGrid ): RecordKey = {
    val start = recordStartIndex(iRecord)
    val startDate = grid.getCalendarDate(start)
    val startDateStr = startDate.toString
    val startTime = startDate.getMillis/1000
    val end = Math.min( start+recordSize, grid.shape(0)-1 )
    val endDate = grid.getCalendarDate(end)
    val endDateStr = endDate.toString
    val endTime =  grid.getCalendarDate(end).getMillis/1000
    RecordKey( startTime, endTime, startIndex, recordSize )
  }

  def recordStartIndex(iRecord: Int) = { origin(0) + iRecord * recordSize + startIndex }
  def recordIndexArray: IndexedSeq[Int] = (0 until nRecords)
}

class FilteredPartition(index: Int, dimIndex: Int, startIndex: Int, partSize: Int, sliceMemorySize: Long, origin: Array[Int], shape: Array[Int], val records: IndexedSeq[FilteredRecordSpec] ) extends Partition( index, dimIndex, startIndex, partSize, sliceMemorySize, origin, shape ) {
  override def recordStartIndex( iRecord: Int ) = records(iRecord).first
  override def recordRange(iRecord: Int): ma2.Range = {
    val start = recordStartIndex(iRecord);
    new ma2.Range( start, origin(0)+records(iRecord).last )
  }
  override def nRecords: Int = records.length
  override val toString: String = s"Part[$index]{dim:$dimIndex start:$startIndex partSize:$partSize sliceMemorySize:$sliceMemorySize origin:${origin.mkString(",")} shape:${shape.mkString(",")} )"

  override def getRecordKey( iRecord: Int, grid: TargetGrid ): RecordKey = {
    val record = records(iRecord)
    val startDate = grid.getCalendarDate(record.first)
    val startDateStr = startDate.toString
    val startTime = startDate.getMillis/1000
    val end = Math.min( record.last+1, grid.shape(0)-1 )
    val endDate = grid.getCalendarDate(end)
    val endDateStr = endDate.toString
    val endTime =  grid.getCalendarDate(end).getMillis/1000
    RecordKey( startTime, endTime, record.first, record.length )
  }
}

import ucar.nc2.time.{CalendarDate, CalendarDateRange, CalendarPeriod}

object TimePeriod {
  object RelativePosition extends Enumeration { val Before, After, FirstHalf, SecondHalf = Value }
  def apply(  first: CalendarDate, last: CalendarDate ): TimePeriod = new TimePeriod( CalendarDateRange.of(first,last) )
}

class TimePeriod( val dateRange: CalendarDateRange ){
  import TimePeriod._
  val durationMS = (dateRange.getEnd.getMillis - dateRange.getStart.getMillis).toDouble

  def relativePosition( date: CalendarDate ): RelativePosition.Value = {
    if( date.isBefore( dateRange.getStart ) ) { RelativePosition.Before }
    else if( date.isBefore( dateRange.getEnd ) ) { RelativePosition.After }
    else {
      val pos = ( date.getMillis - dateRange.getStart.getMillis ) / durationMS
      if( pos < 0.5 ) RelativePosition.FirstHalf else  RelativePosition.SecondHalf
    }
    RelativePosition.Before
  }

}


//class CacheFileReader( val datasetFile: String, val varName: String, val sectionOpt: Option[ma2.Section] = None, val cacheType: String = "fragment" ) extends XmlResource {
//  private val netcdfDataset = NetcdfDataset.openDataset( datasetFile )
// private val ncVariable = netcdfDataset.findVariable(varName)

object CDASPartitioner {
  implicit def int2String(x: Int): String = x.toString
  val M = 1024 * 1024
  val maxRecordSize = 200*M
  val defaultRecordSize = 200*M
  val defaultPartSize = 1000*M
  val secPerDay: Float = ( 60 * 60 * 24 ).toFloat
  val secPeryear: Float  = ( secPerDay * 356 )
  val secPerMonth: Float = secPerMonth / 12
  val recordSize: Float = math.min( cdsutils.parseMemsize( appParameters( "record.size", defaultRecordSize ) ), maxRecordSize ).toFloat
  val partitionSize: Float = math.max( cdsutils.parseMemsize( appParameters( "partition.size", defaultPartSize) ), recordSize )
  val nCoresPerPart = 1
}

class CDASCachePartitioner(val cache_id: String, _section: ma2.Section, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode], timeAxisOpt: Option[CoordinateAxis1DTime], numDataFiles: Int, dataType: ma2.DataType = ma2.DataType.FLOAT, cacheType: String = "fragment") extends CDASPartitioner(_section,partsConfig,workflowNodeOpt,timeAxisOpt,numDataFiles,dataType,cacheType) {

  def getCacheFilePath(partIndex: Int): String = {
    val cache_file = cache_id + "-" + partIndex.toString
    DiskCacheFileMgr.getDiskCacheFilePath(cacheType, cache_file)
  }

  override def getPartition(partIndex: Int): CachePartition = CachePartition( getCacheFilePath(partIndex), spec.getParition(partIndex).asInstanceOf[RegularPartition] )
  def getCachePartitions: Array[CachePartition] = (0 until spec.getNPartitions).map(getPartition(_)).toArray
}

object SeasonFilter {
  val months = "JFMAMJJASONDJFMAMJJASOND"
  def get(filterSpec: String): Option[SeasonFilter] = months.indexOfSlice(filterSpec) match {
    case -1 => None
    case index => Some(new SeasonFilter( (index until (index+filterSpec.length)) map ( i => ( ((i-1)%12)+1, (i-1)/12 ) ) ) )
  }
}

class SeasonFilter( val months: IndexedSeq[(Int,Int)] ) extends Loggable {
  val length = months.length
  private val _records = new java.util.TreeMap[Int,FilteredRecordSpec]()
  def pass( iMonth: Int ): Option[(Int,Int)] = months.find( _._1 == iMonth )
  def getRecordSpec( iYear: Int ): FilteredRecordSpec = _records.getOrElseUpdate( iYear, new FilteredRecordSpec )
  def getRecords = _records.values()
  def getPartitionRecords( startIndex: Int, partSize: Int ) = _records.values().slice( startIndex, startIndex+partSize ).filter( _.length == length )
  def getNRecords = _records.size()
  def processTimestep( timeIndex: Int, date: CalendarDate ): Unit = {
    val iYear = date.getFieldValue(Year)
    val iMonth = date.getFieldValue(Month)
    pass(iMonth) match {
      case Some( (iMonth, iYearOffset) ) => getRecordSpec(iYear-iYearOffset).add( timeIndex )
      case None => Unit
    }
  }
}

class FilteredRecordSpec {
  var first: Int = Int.MaxValue
  var last: Int = -1
  def add( timeIndex: Int ): Unit = {
    first = Math.min( first, timeIndex )
    last = Math.max( last, timeIndex )
  }
  def length = last - first + 1
  override def toString = s"FilteredRecordSpec( first:$first last:$last length:$length )"
}

class CDASPartitionSpec( val _partitions: IndexedSeq[Partition] ) {
  def getParition( index: Int ): Partition = { _partitions(index) }
  def getNPartitions: Int = _partitions.length
}

case class PartitionSpecs( nPartitions: Int, partMemorySize: Long, nSlicesPerRecord: Int, recordMemorySize: Long, nRecordsPerPart: Int, nSlicesPerPart: Int ) {

}

case class PartitionConstraints( partsConfig: Map[String,String] ) {
  private val _numDataFiles: Int = partsConfig.getOrElse("numDataFiles","0").toInt
  private val _numParts: Int = partsConfig.getOrElse("numParts","0").toInt
  val nSlicesPerRecord: Int = partsConfig.getOrElse("numSlicesPerRecord","0").toInt
  val numParts: Int = if( _numParts > 0 ) { _numParts } else {
    val _numfilesPerPart: Double = math.ceil( _numDataFiles / BatchSpec.nParts.toFloat )
    if ( _numfilesPerPart == 0.0 ) 0 else math.ceil( _numDataFiles / _numfilesPerPart ).toInt
  }
}

class CDASPartitioner( private val _section: ma2.Section, val partsConfig: Map[String,String], val workflowNodeOpt: Option[WorkflowNode], timeAxisOpt: Option[CoordinateAxis1DTime], val numDataFiles: Int, dataType: ma2.DataType = ma2.DataType.FLOAT, val cacheType: String = "fragment") extends Loggable {
  import CDASPartitioner._

  val elemSize = dataType.getSize
  val baseShape = _section.getShape
  val sectionMemorySize = getMemorySize()
  val sliceMemorySize: Long = getMemorySize(1)
  val kernelConfig = workflowNodeOpt.map( _.operation.getConfiguration ).getOrElse( Map.empty )
  val filters = kernelConfig.getOrElse("filter","").split(",").filter( !_.isEmpty )
  val sectionRange: ma2.Range = _section.getRange(0)
  lazy val spec = computeRecordSizes()
  lazy val partitions = new Partitions( _section, getPartitions )

  def timeAxis: CoordinateAxis1DTime = {
    val fullAxis = timeAxisOpt getOrElse( throw new Exception( "Missing time axis in Partitioner") )
    fullAxis
  }
  def getCalDateBounds( time_index: Int ): Array[CalendarDate] = timeAxis.getCoordBoundsDate(time_index)
  def getCalDate( time_index: Int ): CalendarDate = timeAxis.getCalendarDate( time_index )
  def getBoundsStartDate( time_index: Int ): CalendarDate =  getCalDateBounds( time_index )(0)
  def getBoundsEndDate( time_index: Int ): CalendarDate =  getCalDateBounds( time_index )(1)

  val sectionCalendarRange: CalendarDateRange = CalendarDateRange.of( getBoundsStartDate( sectionRange.first ), getBoundsEndDate( sectionRange.last ) )
  val secondsPerSection: Long = sectionCalendarRange.getDurationInSecs
  val monthsPerSection: Float = secondsPerSection / secPerMonth
  val daysPerSection: Float = secondsPerSection / secPerDay
  val resolutionInDays: Float = daysPerSection / baseShape(0)
  val monthMemorySize: Float = sectionMemorySize / monthsPerSection
  val slicesPerDay: Float = daysPerSection / baseShape(0)
  val yearsPerSection: Float = secondsPerSection / secPeryear

  def getPartitionSpecs( constraints: PartitionConstraints ): PartitionSpecs = {
    val desiredPartSize = if( constraints.numParts == 0 ) { partitionSize } else { math.min( partitionSize, sectionMemorySize / constraints.numParts.toFloat ) }
    val currentPartitionSize: Float = math.max( desiredPartSize, sliceMemorySize )
    val currentRecordSize: Float = if( constraints.nSlicesPerRecord == 0 ) { math.min( recordSize, currentPartitionSize ) } else { constraints.nSlicesPerRecord * sliceMemorySize }
    val _nSlicesPerRecord: Int = math.max( currentRecordSize / sliceMemorySize, 1.0).round.toInt
    val _recordMemorySize: Long = getMemorySize(_nSlicesPerRecord)
    val _nRecordsPerPart: Int = math.max( currentPartitionSize / _recordMemorySize, 1.0).round.toInt
    val _partMemorySize: Long = _nRecordsPerPart * _recordMemorySize
    var _nSlicesPerPart: Int = _nRecordsPerPart * _nSlicesPerRecord
    val _nPartitions: Int = math.ceil(sectionMemorySize / _partMemorySize.toFloat).toInt
    PartitionSpecs( _nPartitions, _partMemorySize, _nSlicesPerRecord, _recordMemorySize, _nRecordsPerPart, _nSlicesPerPart )
  }

  def getForceNParts(): Int = {
    val forceNParts = partsConfig.getOrElse("numParts","0").toInt
    if( forceNParts > 0 ) { forceNParts }
    else {
      forceNParts
//      val minNumSmallParts = partsConfig.getOrElse("minNumSmallParts","10").toInt
//      val minNParts = math.min( minNumSmallParts, numDataFiles )
//      if( minNParts * partitionSize > sectionMemorySize ) { minNParts }     // TODO: debug this enhancement.
//      else { 0 }
    }
  }

  def computeRecordSizes( ): CDASPartitionSpec = {
    val sparkConfig = BatchSpec.serverContext.spark.sparkContext.getConf.getAll map { case (key, value ) =>  key + " -> " + value } mkString( "\n\t")
    if( filters.isEmpty ) {
      val pSpecs = getPartitionSpecs( PartitionConstraints( partsConfig ++ Map( "numDataFiles" -> numDataFiles.toString ) ) )
      val partitions = (0 until pSpecs.nPartitions) map ( partIndex => {
        val startIndex = partIndex * pSpecs.nSlicesPerPart
        val partSize = Math.min(pSpecs.nSlicesPerPart, baseShape(0) - startIndex)
        RegularPartition(partIndex, 0, startIndex, partSize, pSpecs.nSlicesPerRecord, sliceMemorySize, _section.getOrigin, baseShape)
      })
      logger.info(  s"\n---------------------------------------------\n ~~~~ Generating batched partitions: numDataFiles: ${numDataFiles}, sectionMemorySize: ${sectionMemorySize/M.toFloat} M, sliceMemorySize: ${sliceMemorySize/M.toFloat} M, nSlicesPerRecord: ${pSpecs.nSlicesPerRecord}, recordMemorySize: ${pSpecs.recordMemorySize/M.toFloat} M, nRecordsPerPart: ${pSpecs.nRecordsPerPart}, partMemorySize: ${pSpecs.partMemorySize/M.toFloat} M, nPartitions: ${partitions.length} \n---------------------------------------------\n")
      new CDASPartitionSpec( partitions )
    } else {
      val seasonFilters = filters.flatMap( SeasonFilter.get )
      if( seasonFilters.length < filters.length ) throw new Exception ( "Unrecognized filter: " + filters.mkString(",") )
      val timeSteps: List[CalendarDate] = timeAxis.section(sectionRange).getCalendarDates.toList
      for( (timeStep, timeIndex) <- timeSteps.zipWithIndex; seasonFilter <- seasonFilters ) { seasonFilter.processTimestep( timeIndex, timeStep  ) }
      val all_records = for( seasonFilter <- seasonFilters; record <- seasonFilter.getRecords ) yield { record }
      val partitions: IndexedSeq[Partition] = if( all_records.length <= BatchSpec.nParts ) {
        for( ( record, partIndex ) <- all_records.zipWithIndex ) yield {
          new FilteredPartition(partIndex, 0, record.first, record.length, sliceMemorySize, _section.getOrigin, baseShape, Array(record))
        }
      } else {
        val nRecs = seasonFilters(0).getNRecords
        val seasonRecsPerPartition = Math.ceil( nRecs / BatchSpec.nParts ).toInt
        val nParts = Math.ceil( nRecs / seasonRecsPerPartition.toFloat ).toInt
        val partSize = Math.ceil( baseShape(0)/nParts.toFloat ).toInt
        ( 0 until nParts ) map ( partIndex => {
          val iRecStart = partIndex*seasonRecsPerPartition
          val records = seasonFilters flatMap ( _.getPartitionRecords(iRecStart,seasonRecsPerPartition) )
          new FilteredPartition(partIndex, 0, records.head.first, partSize, sliceMemorySize, _section.getOrigin, baseShape, records )
        } )
      }
      logger.info(  s"\n---------------------------------------------\n ~~~~ Generating partitions for ${BatchSpec.nParts} procs: \n ${partitions.map( _.toString ).mkString( "\n\t" )}  \n---------------------------------------------\n")
      new CDASPartitionSpec( partitions )
    }
  }

//  def computeRecordSizes1( ): CDASPartitionSpec = {
//    val sparkConfig = BatchSpec.serverContext.spark.sparkContext.getConf.getAll map { case (key, value ) =>  key + " -> " + value } mkString( "\n\t")
//    val _preferredNParts = math.ceil( sectionMemorySize / partitionSize.toFloat ).toInt
//    if( _preferredNParts > BatchSpec.nProcessors * 1.5 ) {
//      val _nSlicesPerRecord: Int = math.max(recordSize.toFloat / sliceMemorySize, 1.0).round.toInt
//      val _recordMemorySize: Long = getMemorySize(_nSlicesPerRecord)
//      val _nRecordsPerPart: Int = math.max(partitionSize.toFloat / _recordMemorySize, 1.0).round.toInt
//      val _partMemorySize: Long = _nRecordsPerPart * _recordMemorySize
//      val _nSlicesPerPart: Int = _nRecordsPerPart * _nSlicesPerRecord
//      val _nPartitions: Int = math.ceil(sectionMemorySize / _partMemorySize.toFloat).toInt
//      val partitions = (0 until _nPartitions) map ( partIndex => {
//        val startIndex = partIndex * _nSlicesPerPart
//        val partSize = Math.min(_nSlicesPerPart, baseShape(0) - startIndex)
//        RegularPartition(partIndex, 0, startIndex, partSize, _nSlicesPerRecord, sliceMemorySize, _section.getOrigin, baseShape)
//      })
//      logger.info(  s"\n---------------------------------------------\n ~~~~ Generating batched partitions: preferredNParts: ${_preferredNParts}, sectionMemorySize: $sectionMemorySize, sliceMemorySize: $sliceMemorySize, nSlicesPerRecord: ${_nSlicesPerRecord}, recordMemorySize: ${_recordMemorySize}, nRecordsPerPart: ${_nRecordsPerPart}, partMemorySize: ${_partMemorySize}, nPartitions: ${partitions.length} \n---------------------------------------------\n")
//      new CDASPartitionSpec( partitions )
//    } else {
//      if( filters.isEmpty ) {
//        val __nPartitions: Int = BatchSpec.nProcessors
//        val __nSlicesPerPart: Int = math.ceil(baseShape(0) / __nPartitions.toFloat).toInt
//        val __maxSlicesPerRecord: Int = math.max(math.round(recordSize / sliceMemorySize.toFloat), 1)
//        val __nSlicesPerRecord: Int = math.min( __maxSlicesPerRecord, __nSlicesPerPart )
//        val __nRecordsPerPart: Int = math.ceil(__nSlicesPerPart / __nSlicesPerRecord.toFloat).toInt
//        val __recordMemorySize: Long = getMemorySize(__nSlicesPerRecord)
//        val __partMemorySize: Long = __recordMemorySize * __nRecordsPerPart
//        val partitions = (0 until __nPartitions) flatMap ( partIndex => {
//          val startIndex = partIndex * __nSlicesPerPart
//          val partSize = Math.min(__nSlicesPerPart, baseShape(0) - startIndex)
//          if(partSize>0) { Some( RegularPartition(partIndex, 0, startIndex, partSize, __nSlicesPerRecord, sliceMemorySize, _section.getOrigin, baseShape ) ) } else { None }
//        })
//        logger.info(  s"\n---------------------------------------------\n ~~~~ Generating partitions: nAvailCores: ${__nPartitions}, sectionMemorySize: $sectionMemorySize, sliceMemorySize: $sliceMemorySize, nSlicesPerRecord: ${__nSlicesPerRecord}, recordMemorySize: ${__recordMemorySize}, nRecordsPerPart: ${__nRecordsPerPart}, partMemorySize: ${__partMemorySize}, nPartitions: ${partitions.length} \n---------------------------------------------\n")
//        new CDASPartitionSpec( partitions )
//      } else {
//        val seasonFilters = filters.flatMap( SeasonFilter.get )
//        if( seasonFilters.length < filters.length ) throw new Exception ( "Unrecognized filter: " + filters.mkString(",") )
//        val timeSteps: List[CalendarDate] = timeAxis.section(sectionRange).getCalendarDates.toList
//        for( (timeStep, timeIndex) <- timeSteps.zipWithIndex; seasonFilter <- seasonFilters ) { seasonFilter.processTimestep( timeIndex, timeStep  ) }
//        val all_records = for( seasonFilter <- seasonFilters; record <- seasonFilter.getRecords ) yield { record }
//        val partitions: IndexedSeq[Partition] = if( all_records.length < BatchSpec.nProcessors ) {
//          for( ( record, partIndex ) <- all_records.zipWithIndex ) yield {
//            new FilteredPartition(partIndex, 0, record.first, record.length, sliceMemorySize, _section.getOrigin, baseShape, Array(record))
//          }
//        } else {
//          val nRecs = seasonFilters(0).getNRecords
//          val seasonRecsPerPartition = Math.ceil( nRecs / ( BatchSpec.nProcessors - 1f ) ).toInt
//          val nParts = Math.ceil( nRecs / seasonRecsPerPartition.toFloat ).toInt
//          val partSize = Math.ceil( baseShape(0)/nParts.toFloat ).toInt
//          ( 0 until nParts ) map ( partIndex => {
//            val iRecStart = partIndex*seasonRecsPerPartition
//            val records = seasonFilters flatMap ( _.getPartitionRecords(iRecStart,seasonRecsPerPartition) )
//            new FilteredPartition(partIndex, 0, records.head.first, partSize, sliceMemorySize, _section.getOrigin, baseShape, records )
//          } )
//        }
//        logger.info(  s"\n---------------------------------------------\n ~~~~ Generating partitions for ${BatchSpec.nProcessors} procs: \n ${partitions.map( _.toString ).mkString( "\n\t" )}  \n---------------------------------------------\n")
//        new CDASPartitionSpec( partitions )
//      }
//    }
//  }

  def getShape = baseShape
  def roi: ma2.Section = new ma2.Section(_section.getRanges)

  def getPartition(partIndex: Int): Partition = spec.getParition( partIndex )
  def getPartitions: Array[Partition] = ( 0 until spec.getNPartitions ).map( getPartition ).toArray

  def getMemorySize(nSlices: Int = -1): Long = {
    var full_shape = baseShape.clone()
    if (nSlices > 0) { full_shape(0) = nSlices }
    full_shape.foldLeft(4L)(_ * _)
  }

//  def computePartitions = {
//      if (resolutionInDays > 1.0) {
//          val daysPerChunk = daysPerSection / nChunksPerSection
//          val slicesPerChunk = Math.floor( slicesPerDay * daysPerChunk )
//
//          } else {
//          val monthMemSize = // getMemorySize(356f/12f)
//          val chunksPerMonth = nChunksPerSection / monthsPerSection
//          if (chunksPerMonth < 1.0) {
//              1
//            } else {
//              val nParts = 1
//            }
//        }
//    }

  private def getPeriodRanges( period: CalendarPeriod.Field ): Map[Int,TimePeriod] = {
    var currentPeriod = -1
    var startDate: CalendarDate = CalendarDate.present()
    var currDate: CalendarDate = startDate
    val periodRanges = scala.collection.mutable.HashMap.empty[Int,TimePeriod]
    timeAxis.getCalendarDates.toList.zipWithIndex foreach { case ( date, index ) =>
      val periodIndex = date.getFieldValue( period )
      if( currentPeriod < 0 ) { startDate = date }
      else if( periodIndex != currentPeriod ) {
        periodRanges.put( periodIndex, TimePeriod( startDate, currDate ) )
        startDate = date
      }
      currDate = date
      currentPeriod = periodIndex
    }
    periodRanges.toMap
  }

  private def getNearestMonthBoundary( time_index: Int  ): Int = {
    val startDate = getCalDate( time_index )
    val dom = startDate.getDayOfMonth()
    val month = if( dom > 15 ) startDate
    time_index
  }
}

class FileToCacheStream(val fragmentSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode], val maskOpt: Option[CDByteArray], val cacheType: String = "fragment") extends Loggable {
  val attributes = fragmentSpec.getVariableMetadata
  val _section = fragmentSpec.roi
  val missing_value: Float = getAttributeValue("missing_value", "") match { case "" => Float.MaxValue; case x => x.toFloat }
  protected val baseShape = _section.getShape
  val cacheId = "a" + System.nanoTime.toHexString
  val sType = getAttributeValue("dtype", "FLOAT")
  val dType = ma2.DataType.getType( sType )
  def roi: ma2.Section = new ma2.Section(_section.getRanges)
  val partitioner = new CDASCachePartitioner(cacheId, roi, partsConfig, workflowNodeOpt, fragmentSpec.getTimeCoordinateAxis, fragmentSpec.numDataFiles, dType)
  def getAttributeValue(key: String, default_value: String) =
    attributes.get(key) match {
      case Some(attr_val) => attr_val.toString.split('=').last.replace('"',' ').trim
      case None => default_value
    }

  def getReadBuffer(cache_id: String): (FileChannel, MappedByteBuffer) = {
    val channel = new FileInputStream(cache_id).getChannel
    val size = math.min(channel.size, Int.MaxValue).toInt
    ( channel, channel.map(FileChannel.MapMode.READ_ONLY, 0, size))
  }

  def cacheFloatData: CachePartitions = {
    assert(dType == ma2.DataType.FLOAT,  "Attempting to cache %s data as float".format(dType.toString))
    execute(missing_value)
  }

  def execute(missing_value: Float): CachePartitions = {
    val t0 = System.nanoTime()
    val future_partitions: IndexedSeq[Future[CachePartition]] = for ( pIndices <- ( 0 until partitioner.spec.getNPartitions ) ) yield Future { processChunkedPartitions(cacheId, pIndices, missing_value) }
    val partitions: Array[CachePartition] = Await.result(Future.sequence(future_partitions.toList), Duration.Inf).toArray
    logger.info("\n ********** Completed Cache Op, total time = %.3f min  ********** \n".format((System.nanoTime() - t0) / 6.0E10))
    new CachePartitions( cacheId, roi, partitions)
  }

  def processChunkedPartitions(cache_id: String, partIndex: Int, missing_value: Float): CachePartition = {
    logger.info( "Process Chunked Partitions(%s): %d".format( cache_id, partIndex ) )
    val partition: CachePartition = partitioner.getPartition(partIndex);
    val outStr = new BufferedOutputStream( new FileOutputStream(new File(partition.path)))
    cachePartition(partition, outStr)
    outStr.close
    partition
  }

  def cacheRecord(partition: RegularPartition, iRecord: Int, outStr: BufferedOutputStream) = {
    logger.info( "CacheRecord: part=%d, record=%d".format(partition.index, iRecord))
    val subsection: ma2.Section = partition.recordSection(roi,iRecord)
    val t0 = System.nanoTime()
    logger.info( " ---> Reading data record %d, part %d, startTimIndex = %d, shape [%s], subsection [%s:%s], nElems = %d ".format(iRecord, partition.index, partition.startIndex, getAttributeValue("shape", ""), subsection.getOrigin.mkString(","), subsection.getShape.mkString(","), subsection.getShape.foldLeft(1L)(_ * _)))
    val data = fragmentSpec.readData( subsection )
    val chunkShape = data.getShape
    val dataBuffer = data.getDataAsByteBuffer
    val t1 = System.nanoTime()
    logger.info( "Finished Reading data record %d, shape = [%s], buffer capacity = %.2f M in time %.2f ".format(iRecord, chunkShape.mkString(","), dataBuffer.capacity() / 1.0E6, (t1 - t0) / 1.0E9))
    val t2 = System.nanoTime()
    IOUtils.write(dataBuffer.array(), outStr)
    val t3 = System.nanoTime()
    logger.info( " -----> Writing record %d, write time = %.3f " .format(iRecord, (t3 - t2) / 1.0E9))
    val t4 = System.nanoTime()
    logger.info( s"Persisted record %d, write time = %.2f " .format(iRecord, (t4 - t3) / 1.0E9))
    runtime.printMemoryUsage(logger)
  }

  def cachePartition(partition: RegularPartition, stream: BufferedOutputStream) = {
    logger.info( "Caching Partition(%d): chunk start indices: (%s), roi: %s".format( partition.index, partition.recordIndexArray.map(partition.recordStartIndex).mkString(","), baseShape.mkString(",")))
    for (iRecord <- partition.recordIndexArray; startLoc = partition.recordStartIndex(iRecord); if startLoc <= _section.getRange(0).last())
      yield cacheRecord(partition, iRecord, stream)
  }
}

object FragmentPersistence extends DiskCachable with FragSpecKeySet {
  private val fragmentIdCache: Cache[String, String] =
    new FutureCache("CacheIdMap", "fragment", true)
  val M = 1000000

  def getCacheType = "fragment"
  def keys: Set[String] = fragmentIdCache.keys
  def keyObjects: Set[String] = fragmentIdCache.keys
  def values: Iterable[Future[String]] = fragmentIdCache.values

//  def persist(fragSpec: DataFragmentSpec, frag: PartitionedFragment): Future[String] = {
//    val keyStr =  fragSpec.getKey.toStrRep
//    fragmentIdCache.get(keyStr) match {
//      case Some(fragIdFut) => fragIdFut
//      case None => fragmentIdCache(keyStr) {
//        val fragIdFut = promiseCacheId(frag) _
//        fragmentIdCache.persist()
//        fragIdFut
//      }
//    }
//  }

  def expandKey(fragKey: String): String = {
    val toks = fragKey.split('|')
    "variable= %s; origin= (%s); shape= (%s); coll= %s; maxParts=%s".format(toks(0), toks(2), toks(3), toks(1), toks(4))
  }

  def expandKeyXml(fragKey: String): xml.Elem = {
    val toks = fragKey.split('|')
    <fragment variable={toks(0)} origin={toks(2)} shape={toks(3)} coll={toks(1)} maxParts={toks(4)}/> // { getBounds(fragKey) } </fragment>
  }

  def contractKey(fragDescription: String): String = {
    val tok = fragDescription
      .split(';')
      .map(_.split('=')(1).trim.stripPrefix("(").stripSuffix(")"))
    Array(tok(0), tok(3), tok(1), tok(2), tok(4)).mkString("|")
  }

  def fragKeyLT(fragKey1: String, fragKey2: String): Boolean = {
    val toks1 = fragKey1.split('|')
    val toks2 = fragKey2.split('|')
    (toks1(1) + toks1(0)) < (toks2(1) + toks2(0))
  }

  def getFragmentListXml(): xml.Elem =
    <fragments> { for(fkey <- fragmentIdCache.keys.toIndexedSeq.sortWith(fragKeyLT) ) yield expandKeyXml(fkey) } </fragments>
  def getFragmentIdList(): Array[String] = fragmentIdCache.keys.toArray
  def getFragmentList(): Array[String] =
    fragmentIdCache.keys.map(k => expandKey(k)).toArray
  def put(key: DataFragmentKey, cache_id: String) = {
    fragmentIdCache.put(key.toStrRep, cache_id); fragmentIdCache.persist()
  }
  def getEntries: Seq[(String, String)] = fragmentIdCache.getEntries

//  def promiseCacheId( frag: PartitionedFragment )(p: Promise[String]): Unit = {
//    try {
//      val cacheFile = bufferToDiskFloat(frag.data.getSectionData())
//      p.success(cacheFile)
//    }  catch {
//      case err: Throwable => logError(err, "Error writing cache data to disk:"); p.failure(err)
//    }
//  }\\

  def restore( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode]): Option[Future[PartitionedFragment]] = {
    val fragKey = fragSpec.getKey
//    logger.info("FragmentPersistence.restore: fragKey: " + fragKey)
    findEnclosingFragmentData(fragSpec) match {
      case Some(foundFragKey) =>
        collectionDataCache.getFragment(foundFragKey) match {
          case Some(partFut) => Some(partFut)
          case None =>
            fragmentIdCache.get(foundFragKey) match {
              case Some(cache_id_fut) =>
                Some(cache_id_fut.map((cache_id: String) => {
                  val roi = DataFragmentKey(foundFragKey).getRoi
                  val partitioner = new CDASCachePartitioner( cache_id, roi, partsConfig, workflowNodeOpt, fragSpec.getTimeCoordinateAxis, fragSpec.numDataFiles )
                  fragSpec.cutIntersection(roi) match {
                    case Some(section) =>
                      new PartitionedFragment( new CachePartitions( cache_id, roi, partitioner.getCachePartitions ), None, section)
                    case None =>
                      throw new Exception(
                        "No intersection in enclosing fragment")
                  }
                }))
              case None => None
            }
        }
      case None =>
        logger.info("Can't find enclosing fragKey")
        None
    }
  }

//  def restore( ): Unit = {
//    restorePartitions( cache_id: String, nParts: Int, missing_value: Float )
//  }
//  def restore( cache_id: String, size: Int ): Option[PartitionedFragment] = bufferFromDiskFloat( cache_id, size )
//  def restore( fragKey: DataFragmentKey ): Option[PartitionedFragment] =  fragmentIdCache.get(fragKey.toStrRep).flatMap( restore( _, fragKey.getSize ) )
//  def restore( cache_id_future: Future[String], size: Int ): Option[PartitionedFragment] = restore( Await.result(cache_id_future,Duration.Inf), size )
//  def getFragmentData( fragSpec: DataFragmentSpec ): Option[ FloatBuffer ] = restore( fragSpec.getKey )

  def close(): Unit =
    Await.result(Future.sequence(fragmentIdCache.values), Duration.Inf)

  def clearCache(): Set[String] = fragmentIdCache.clear()
  def deleteEnclosing(fragSpec: DataFragmentSpec) = delete(findEnclosingFragSpecs(fragmentIdCache.keys, fragSpec.getKey))
  def findEnclosingFragmentData(fragSpec: DataFragmentSpec): Option[String] = findEnclosingFragSpecs(fragmentIdCache.keys, fragSpec.getKey).headOption

  def delete( fragKeys: Iterable[String] ) = {
    for (fragKey <- fragKeys; cacheFragKey <- fragmentIdCache.keys; if cacheFragKey.startsWith(fragKey); cache_id_future = fragmentIdCache.get(cacheFragKey).get) {
      val path = DiskCacheFileMgr.getDiskCacheFilePath(getCacheType, Await.result(cache_id_future, Duration.Inf))
      fragmentIdCache.remove(cacheFragKey)
      val matcher: java.nio.file.PathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + path + "*")
      val fileFilter: java.io.FileFilter = new FileFilter() { override def accept(pathname: File): Boolean = {matcher.matches(pathname.toPath) } }
      val parent = new File(path).getParentFile
      for (file <- parent.listFiles(fileFilter)) {
        if (file.delete) logger.info(s"Deleting persisted fragment file " + file.getAbsolutePath + ", frag: " + cacheFragKey)
        else logger.warn(s"Failed to delete persisted fragment file " + file.getAbsolutePath)
      }
    }
    fragmentIdCache.persist()
  }
}

trait FragSpecKeySet extends nasa.nccs.utilities.Loggable {

  def getFragSpecsForVariable(keys: Set[String], collection: String, varName: String): Set[DataFragmentKey] =
    keys.filter( _ match {
        case fkey: String =>
          DataFragmentKey(fkey).sameVariable(collection, varName)
        case x =>
          logger.warn("Unexpected fragment key type: " + x.getClass.getName);
          false
      }).map(_ match { case fkey: String => DataFragmentKey(fkey) })

  def findEnclosingFragSpecs(keys: Set[String],
                             fkey: DataFragmentKey,
                             admitEquality: Boolean = true): List[String] = {
    val variableFrags = getFragSpecsForVariable(keys, fkey.collId, fkey.varname)
    variableFrags
      .filter(fkeyParent => fkeyParent.contains(fkey, admitEquality))
      .toList
      .sortWith(_.getSize < _.getSize)
      .map(_.toStrRep)
  }

  def findEnclosedFragSpecs(keys: Set[String],
                            fkeyParent: DataFragmentKey,
                            admitEquality: Boolean = false): Set[String] = {
    val variableFrags =
      getFragSpecsForVariable(keys, fkeyParent.collId, fkeyParent.varname)
    logger.info(
      "Searching variable frags: \n\t --> " + variableFrags.mkString(
        "\n\t --> "))
    variableFrags
      .filter(fkey => fkeyParent.contains(fkey, admitEquality))
      .map(_.toStrRep)
  }

  def findEnclosingFragSpec(keys: Set[String],
                            fkeyChild: DataFragmentKey,
                            selectionCriteria: FragmentSelectionCriteria.Value,
                            admitEquality: Boolean = true): Option[String] = {
    val enclosingFragments =
      findEnclosingFragSpecs(keys, fkeyChild, admitEquality)
    if (enclosingFragments.isEmpty) None
    else
      Some(selectionCriteria match {
        case FragmentSelectionCriteria.Smallest =>
          enclosingFragments.minBy(DataFragmentKey(_).getRoi.computeSize())
        case FragmentSelectionCriteria.Largest =>
          enclosingFragments.maxBy(DataFragmentKey(_).getRoi.computeSize())
      })
  }
}

class JobRecord(val id: String) {
  override def toString: String = s"ExecutionRecord[id=$id]"
  def toXml: xml.Elem = <job id={id} />
}

class RDDTransientVariable(val result: RDDRecord,
                           val operation: OperationContext,
                           val request: RequestContext) {
  val timeFormatter = new SimpleDateFormat("MM.dd-HH:mm:ss")
  val timestamp = Calendar.getInstance().getTime

  def getTimestamp = timeFormatter.format(timestamp)

  def getGridId = result.metadata.get("gid") match {
    case Some(x) => x;
    case None => throw new Exception("Error, no gridId in result of operation " + operation.identifier)
  }
}

class TransientDataCacheMgr extends Loggable {
  private val transientFragmentCache: Cache[String, TransientFragment] =
    new FutureCache("Store", "result", false)

  def putResult(resultId: String,
                resultFut: Future[Option[TransientFragment]]) =
    resultFut.onSuccess {
      case resultOpt =>
        resultOpt.map(result => transientFragmentCache.put(resultId, result))
    }

  def getResultListXml(): xml.Elem =
    <results> { for( rkey <- transientFragmentCache.keys ) yield <result type="fragment" id={rkey} /> } </results>
  def getResultIdList = transientFragmentCache.keys

  def deleteResult(resultId: String): Option[Future[TransientFragment]] =
    transientFragmentCache.remove(resultId)
  def getExistingResult(resultId: String): Option[Future[TransientFragment]] = {
    val result: Option[Future[TransientFragment]] =
      transientFragmentCache.get(resultId)
    logger.info(
      ">>>>>>>>>>>>>>>> Get result from cache: search key = " + resultId + ", existing keys = " + transientFragmentCache.keys.toArray
        .mkString("[", ",", "]") + ", Success = " + result.isDefined.toString)
    result
  }
}
class CollectionDataCacheMgr extends nasa.nccs.esgf.process.DataLoader with FragSpecKeySet {
  val K = 1000f
  private val fragmentCache: Cache[String, PartitionedFragment] = new FutureCache[String, PartitionedFragment]("Store", "fragment", false) {
      override def evictionNotice(key: String, value: Future[PartitionedFragment]) = {
        value.onSuccess {
          case pfrag =>
            logger.info("Clearing fragment %s".format(key));
            FragmentPersistence.delete(List(key))
            pfrag.delete
        }
      }
      override def entrySize(key: String,  value: Future[PartitionedFragment]): Int = {
        math.max(((DataFragmentKey(key).getSize * 4) / K).round, 1)
      }
    }
  private val rddPartitionCache: ConcurrentLinkedHashMap[ String, RDDTransientVariable] =
    new ConcurrentLinkedHashMap.Builder[String, RDDTransientVariable]
      .initialCapacity(64)
      .maximumWeightedCapacity(128)
      .build()
  private val execJobCache =
    new ConcurrentLinkedHashMap.Builder[String, JobRecord]
      .initialCapacity(64)
      .maximumWeightedCapacity(128)
      .build()

  private val maskCache: Cache[MaskKey, CDByteArray] = new FutureCache("Store", "mask", false)

  def clearFragmentCache() = fragmentCache.clear
  def addJob(jrec: JobRecord): String = {
    execJobCache.put(jrec.id, jrec); jrec.id
  }
  def removeJob(jid: String) = execJobCache.remove(jid)

  def getFragmentList: Array[String] = fragmentCache.getEntries.map {
      case (key, frag) => "%s, bounds:%s".format(key, frag.toBoundsString)
    } toArray

  def makeKey(collection: String, varName: String) = collection + ":" + varName
  def keys: Set[String] = fragmentCache.keys
  def values: Iterable[Future[PartitionedFragment]] = fragmentCache.values

  def extractFuture[T](key: String, result: Option[Try[T]]): T = result match {
    case Some(tryVal) =>
      tryVal match {
        case Success(x) => x;
        case Failure(t) => throw t
      }
    case None => throw new Exception(s"Error getting cache value $key")
  }

  def getExistingResult(resultId: String): Option[RDDTransientVariable] = {
    val result: Option[RDDTransientVariable] = Option(
      rddPartitionCache.get(resultId))
    logger.info(
      ">>>>>>>>>>>>>>>> Get result from cache: search key = " + resultId + ", existing keys = " + rddPartitionCache.keys.toArray
        .mkString("[", ",", "]") + ", Success = " + result.isDefined.toString)
    result
  }
  def putResult(resultId: String, result: RDDTransientVariable) = rddPartitionCache.put(resultId, result)

  def getResultListXml(): xml.Elem =
    <results>
      { rddPartitionCache.map { case (rkey,rval) =>  { <result type="rdd" id={rkey} timestamp={rval.getTimestamp} /> } } }
    </results>

  def getResultIdList = rddPartitionCache.keys

  def deleteResult(resultId: String): RDDTransientVariable = rddPartitionCache.remove(resultId)

  def getJobListXml(): xml.Elem =
    <jobs>
      { for( jrec: JobRecord <- execJobCache.values ) yield jrec.toXml }
    </jobs>


//  private def cutExistingFragment( partIndex: Int, fragSpec: DataFragmentSpec, abortSizeFraction: Float=0f ): Option[DataFragment] = {
//    getExistingFragment(fragSpec) match {
//      case Some(fragmentFuture) =>
//        if (!fragmentFuture.isCompleted && (fragSpec.getSize * abortSizeFraction > fragSpec.getSize)) {
//          logger.info("Cache Chunk[%s] found but not yet ready, abandoning cache access attempt".format(fragSpec.getShape.mkString(",")))
//          None
//        } else {
//          val fragment = Await.result(fragmentFuture, Duration.Inf)
//          fragment.cutIntersection(partIndex, fragSpec.roi)
//        }
//      case None => cutExistingFragment(partIndex, fragSpec, abortSizeFraction)
//    }
//  }
//    fragOpt match {
//      case None =>
//        FragmentPersistence.getEnclosingFragmentData(fragSpec) match {
//          case Some((fkey, fltBuffer)) =>
//            val cdvar: CDSVariable = getVariable(fragSpec.collection, fragSpec.varname )
//            val newFragSpec = fragSpec.reSection(fkey)
//            val maskOpt = newFragSpec.mask.flatMap( maskId => produceMask( maskId, newFragSpec.getBounds, newFragSpec.getGridShape, cdvar.getTargetGrid( newFragSpec ).getAxisIndices("xy") ) )
//            val fragment = new DataFragment( newFragSpec, new CDFloatArray( newFragSpec.getShape, fltBuffer, cdvar.missing ), partIndex, maskOpt  )
//            fragmentCache.put( fkey, fragment )
//            Some(fragment.cutIntersection(partIndex,fragSpec.roi))
//          case None => None
//        }
//      case x => x
//    }

  private def cacheFragmentFromFilesFut( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode] )( p: Promise[PartitionedFragment] ): Unit =
    try {
      val result = cacheFragmentFromFiles(fragSpec, partsConfig, workflowNodeOpt )
      p.success(result)
    } catch { case e: Exception => p.failure(e) }


  private def cacheFragmentFromFiles(fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode]): PartitionedFragment = {
    val t0 = System.nanoTime()
    val result: PartitionedFragment = fragSpec.targetGridOpt match {
      case Some(targetGrid) =>
        val maskOpt = fragSpec.mask.flatMap(maskId => produceMask(maskId, fragSpec.getBounds(), fragSpec.getGridShape, targetGrid.getAxisIndices("xy").args))
        targetGrid.loadFileDataIntoCache( fragSpec, partsConfig, workflowNodeOpt, maskOpt)
      case None =>
        throw new Exception( "Missing target grid for fragSpec: " + fragSpec.toString )
//        val targetGrid = new TargetGrid( new CDSVariable(),  Some(fragSpec.getAxes) )
//        val maskOpt = fragSpec.mask.flatMap(maskId => produceMask(maskId, fragSpec.getBounds, fragSpec.getGridShape, targetGrid.getAxisIndices("xy").args))
//        targetGrid.loadFileDataIntoCache( fragSpec, maskOpt)
    }
    logger.info( "Completed variable (%s:%s) subset data input in time %.4f sec, section = %s " .format(fragSpec.collection, fragSpec.varname, (System.nanoTime() - t0) / 1.0E9, fragSpec.roi))
    result
  }

  def produceMask(maskId: String,
                  bounds: Array[Double],
                  mask_shape: Array[Int],
                  spatial_axis_indices: Array[Int]): Option[CDByteArray] = {
    if (Masks.isMaskId(maskId)) {
      val maskFuture =
        getMaskFuture(maskId, bounds, mask_shape, spatial_axis_indices)
      val result = Await.result(maskFuture, Duration.Inf)
      logger.info("Loaded mask (%s) data".format(maskId))
      Some(result)
    } else {
      None
    }
  }

  private def getMaskFuture(
      maskId: String,
      bounds: Array[Double],
      mask_shape: Array[Int],
      spatial_axis_indices: Array[Int]): Future[CDByteArray] = {
    val fkey = MaskKey(bounds, mask_shape, spatial_axis_indices)
    val maskFuture = maskCache(fkey) {
      promiseMask(maskId, bounds, mask_shape, spatial_axis_indices) _
    }
    logger.info(
      ">>>>>>>>>>>>>>>> Put mask in cache: " + fkey.toString + ", keys = " + maskCache.keys
        .mkString("[", ",", "]"))
    maskFuture
  }

  private def promiseMask(
      maskId: String,
      bounds: Array[Double],
      mask_shape: Array[Int],
      spatial_axis_indices: Array[Int])(p: Promise[CDByteArray]): Unit =
    try {
      Masks.getMask(maskId) match {
        case Some(mask) =>
          mask.mtype match {
            case "shapefile" =>
              val geotools = new GeoTools()
              p.success(
                geotools.produceMask(mask.getPath,
                                     bounds,
                                     mask_shape,
                                     spatial_axis_indices))
            case x => p.failure(new Exception(s"Unrecognized Mask type: $x"))
          }
        case None =>
          p.failure(
            new Exception(
              s"Unrecognized Mask ID: $maskId: options are %s".format(
                Masks.getMaskIds)))
      }
    } catch { case e: Exception => p.failure(e) }

  private def clearRedundantFragments(fragSpec: DataFragmentSpec) =
    findEnclosedFragSpecs(fragmentCache.keys, fragSpec.getKey).foreach(fragmentCache.remove(_))

  def cacheFragmentFuture( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode]): Future[PartitionedFragment] = {
    val keyStr = fragSpec.getKey.toStrRep
    val cacheExecutionMode = appParameters("cache.execution.mode","futures")
    val resultFut = if( cacheExecutionMode.toLowerCase.startsWith("fut") ) {
      val fragFuture = fragmentCache(keyStr) {
        cacheFragmentFromFilesFut(fragSpec,partsConfig,workflowNodeOpt) _
      }
      fragFuture onComplete {
        case Success(fragment) => logger.info(" cache fragment: Success ")
        case Failure(err) => logger.warn(" Failed to cache fragment due to error: " + err.getMessage)
      }
      fragFuture
    } else if( cacheExecutionMode.toLowerCase.startsWith("ser") ) {
      fragmentCache.get(keyStr) match {
        case Some(fragFuture) => fragFuture
        case None =>
          val result = cacheFragmentFromFiles(fragSpec, partsConfig, workflowNodeOpt)
          Future(result)
      }
    } else { throw new Exception( "Unrecognized cacheExecutionMode: " + cacheExecutionMode ) }
    logger.info( ">>>>>>>>>>>>>>>> Put frag in cache: " + fragSpec.toString + ", keys = " + fragmentCache.keys .mkString("[", ",", "]"))
    resultFut
  }

//  def getFragment( partIndex: Int, fragSpec: DataFragmentSpec, dataAccessMode: DataAccessMode, abortSizeFraction: Float=0f  ): DataFragment = {
//    cutExistingFragment( partIndex, fragSpec, abortSizeFraction) getOrElse {
//      val fragmentFuture = getFragmentFuture(fragSpec, dataAccessMode)
//      val result = Await.result(fragmentFuture, Duration.Inf)
//      logger.info("Loaded variable (%s:%s) subset data, section = %s ".format(fragSpec.collection, fragSpec.varname, fragSpec.roi))
//      result.dataFragment(partIndex)
//    }
//  }
//
//  def getFragmentAsync( partIndex: Int, fragSpec: DataFragmentSpec, dataAccessMode: DataAccessMode  ): Future[DataFragment] = {
//    cutExistingFragment( partIndex, fragSpec ) getOrElse {
//      val fragmentFuture = getFragmentFuture(fragSpec, dataAccessMode)
//      fragmentFuture.map( df => df.dataFragment(partIndex) )
//    }
//  }
//
//  def getFragmentAsync1( partIndex: Int, fragSpec: DataFragmentSpec, dataAccessMode: DataAccessMode  ): Future[DataFragment] =
//    cutExistingFragment( partIndex, fragSpec ) match {
//      case Some(fragment) => Future { fragment }
//      case None => getFragmentFuture(fragSpec, dataAccessMode)
//    }

  //  def loadOperationInputFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[DataFragmentSpec] = {
  //    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
  //    variableFuture.flatMap( variable => {
  //      val section = variable.getSubSection(domain_container.axes)
  //      val fragSpec = variable.createFragmentSpec( section, domain_container.mask )
  //      val axisSpecs: AxisIndices = variable.getAxisIndices(dataContainer.getOpSpecs)
  //      for (frag <- getFragmentFuture(fragSpec)) yield new DataFragmentSpec( fragSpec, axisSpecs)
  //    })
  //  }
  //
  //  def loadDataFragmentFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[PartitionedFragment] = {
  //    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
  //    variableFuture.flatMap( variable => {
  //      val section = variable.getSubSection(domain_container.axes)
  //      val fragSpec = variable.createFragmentSpec( section, domain_container.mask )
  //      for (frag <- getFragmentFuture(fragSpec)) yield frag
  //    })
  //  }

  def getExistingMask(fkey: MaskKey): Option[Future[CDByteArray]] = {
    val rv: Option[Future[CDByteArray]] = maskCache.get(fkey)
    logger.info(
      ">>>>>>>>>>>>>>>> Get mask from cache: search key = " + fkey.toString + ", existing keys = " + maskCache.keys
        .mkString("[", ",", "]") + ", Success = " + rv.isDefined.toString)
    rv
  }

  def getFragment(fragKey: String): Option[Future[PartitionedFragment]] =
    fragmentCache.get(fragKey)

  def getExistingFragment( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode] ): Option[Future[PartitionedFragment]] = {
    val fkey = fragSpec.getKey.toStrRep
    if (!fragmentCache.keys.contains(fkey)) {
//      logger.info("Restoring frag from cache: " + fkey.toString )
      FragmentPersistence.restore(fragSpec, partsConfig, workflowNodeOpt) match {
        case Some(partFragFut) =>
          val partFrag = Await.result(partFragFut, Duration.Inf)
          logger.info(" fragmentCache.put, fkey = " + fkey)
          fragmentCache.put(fkey, partFrag)
          Some(partFragFut)
        case None =>
          logger.warn("Unable to restore frag from cache: " + fkey.toString + "\n --> Frags in cache: " + FragmentPersistence.keys.mkString(", "))
          None
      }
    } else {
      val rv = fragmentCache.get(fkey)
      logger.info( ">>>>>>>>>>>>>>>> Get frag from cache: search key = " + fkey.toString + ", existing keys = " + fragmentCache.keys
          .mkString("[", ",", "]") + ", Success = " + rv.isDefined.toString)
      rv
    }
  }

  def deleteFragments( fragIds: Iterable[String] ) = {
    for( skey <- fragmentCache.keys; fkey <- fragIds; if skey.startsWith(fkey) ) fragmentCache.remove(skey)
    logger.info( "Deleting Fragments: %s, Current fragments: %s".format( fragIds.mkString(","), fragmentCache.keys.mkString(",") ) )
    FragmentPersistence.delete(fragIds)
  }

  def clearCache: Set[String] = {
    val fragKeys = fragmentCache.keys
    fragmentCache.clear()
    logger.info( "Deleting All Fragments, Current fragments: %s".format( fragmentCache.keys.mkString(",") ) )
    FragmentPersistence.clearCache()
    fragKeys
  }

}

object collectionDataCache extends CollectionDataCacheMgr()

// class FileToCacheStream1( val ncVariable: nc2.Variable, val roi: ma2.Section, val maskOpt: Option[CDByteArray], val cacheType: String = "fragment"  ) extends Loggable {
//  private val chunkCache = new ConcurrentLinkedHashMap.Builder[Int,CacheChunk].initialCapacity(500).maximumWeightedCapacity(1000000).build()
//  private val nReadProcessors = 3
//  private val baseShape = roi.getShape
//  private val dType: ma2.DataType  = ncVariable.getDataType
//  private val elemSize = ncVariable.getElementSize
//  private val range0 = roi.getRange(0)
//  private val maxBufferSize = Int.MaxValue
//  private val maxChunkSize = 250000000
//  private val throttleSize = 2
//  private val sliceMemorySize: Int = getMemorySize(1)
//  private val slicesPerChunk: Int = if(sliceMemorySize >= maxChunkSize ) 1 else  math.min( ( maxChunkSize / sliceMemorySize ), baseShape(0) )
//  private val chunkMemorySize: Int = if(sliceMemorySize >= maxChunkSize ) sliceMemorySize else getMemorySize(slicesPerChunk)
//  private val nChunks = maxBufferSize/chunkMemorySize
//  private val nSlices = nChunks * slicesPerChunk
//
//  def getMemorySize( nSlices: Int): Int = {
//    var full_shape = baseShape.clone()
//    full_shape(0) = nSlices
//    full_shape.foldLeft(elemSize)(_ * _)
//  }
//
//  def getTruncatedArrayShape(): Array[Int] = {
//    var full_shape = baseShape.clone()
//    full_shape(0) = math.min( nSlices, full_shape(0) )
//    full_shape
//  }
//
//  def getCacheFilePath: String = {
//    val cache_file = "a" + System.nanoTime.toHexString
//    DiskCacheFileMgr.getDiskCacheFilePath(cacheType, cache_file)
//  }
//
//  @tailrec
//  private def throttle: Unit = {
//    val cvals: Iterable[CacheChunk] = chunkCache.values.toIterable
////    val csize = cvals.foldLeft( 0L )( _ + _.byteSize )
//    val num_cached_chunks = cvals.size
//    if( num_cached_chunks >= throttleSize ) {
//      Thread.sleep(5000)
//      throttle
//    }
//  }
//
//  def readDataChunks( coreIndex: Int ): Int = {
//    var subsection = new ma2.Section(roi)
//    logger.info( s" ~~~~~~~~ ReadDataChunks, nReadProcessors = $nReadProcessors, nChunks = $nChunks, coreIndex = $coreIndex" )
//    var nElementsWritten = 0
//    for( iChunk <- (coreIndex until nChunks by nReadProcessors); startLoc = iChunk*slicesPerChunk; if(startLoc < baseShape(0)) ) {
//      logger.info( " ~~~~~~~~ Reading data chunk %d".format( iChunk ) )
//      val endLoc = Math.min( startLoc + slicesPerChunk - 1, baseShape(0)-1 )
//      val chunkRange = new ma2.Range( startLoc, endLoc )
//      subsection.replaceRange(0,chunkRange)
//      val t0 = System.nanoTime()
//      logger.info( " ~~~~~~~~ Reading data chunk %d, startTimIndex = %d, subsection [%s], nElems = %d ".format( iChunk, startLoc, subsection.getShape.mkString(","), subsection.getShape.foldLeft(1L)( _ * _ ) ) )
//      val data = ncVariable.read(subsection)
//      val chunkShape = subsection.getShape
//      val dataBuffer = data.getDataAsByteBuffer
//
//      val chunk = new CacheChunk( startLoc, elemSize, chunkShape, dataBuffer )
//      chunkCache.put( iChunk, chunk )
//      val t1 = System.nanoTime()
//      logger.info( " ~~~~~~~~ Finished Reading data chunk %d, shape = [%s], buffer capacity = %d in time %.2f ".format( iChunk, chunkShape.mkString(","), dataBuffer.capacity(), (t1-t0)/1.0E9 ) )
//      throttle
//      nElementsWritten += chunkShape.product
//    }
//    nElementsWritten
//  }
//
//  def execute(): String = {
//    logger.info( s" ~~~~~~~~ initiate Cache Stream, nReadProcessors = $nReadProcessors, nChunks = $nChunks" )
//    val readProcFuts: IndexedSeq[Future[Int]] = for( coreIndex <- (0 until Math.min( nChunks, nReadProcessors ) ) ) yield Future { readDataChunks(coreIndex) }
//    writeChunks
//  }
//
//  def cacheFloatData( missing_value: Float  ): ( String, CDFloatArray ) = {
//    assert( dType == ma2.DataType.FLOAT, "Attempting to cache %s data as float".format( dType.toString ) )
//    val cache_id = execute()
//    getReadBuffer(cache_id) match {
//      case (channel, buffer) =>
//        val storage: FloatBuffer = buffer.asFloatBuffer
//        channel.close
//        ( cache_id -> new CDFloatArray( getTruncatedArrayShape, storage, missing_value ) )
//    }
//  }
//
//  def getReadBuffer( cache_id: String ): ( FileChannel, MappedByteBuffer ) = {
//    val channel = new FileInputStream( cache_id ).getChannel
//    val size = math.min( channel.size, Int.MaxValue ).toInt
//    channel -> channel.map(FileChannel.MapMode.READ_ONLY, 0, size )
//  }
//
//  def writeChunks: String = {
//    val cacheFilePath = getCacheFilePath
//    val channel = new RandomAccessFile(cacheFilePath,"rw").getChannel()
//    logger.info( "Writing Buffer file '%s', nChunks = %d, chunkMemorySize = %d, slicesPerChunk = %d".format( cacheFilePath, nChunks, chunkMemorySize, slicesPerChunk  ))
//    (0.toInt until nChunks).foreach( processChunkFromReader( _, channel ) )
//    cacheFilePath
//  }
//
//  @tailrec
//  final def processChunkFromReader( iChunk: Int, channel: FileChannel ): Unit = {
//    Option(chunkCache.get(iChunk)) match {
//      case Some( cacheChunk: CacheChunk ) =>
//        val t0 = System.nanoTime()
//        val position: Long = iChunk.toLong * chunkMemorySize.toLong
//        var buffer: MappedByteBuffer = channel.map( FileChannel.MapMode.READ_WRITE, position, chunkMemorySize  )
//        val t1 = System.nanoTime()
//        logger.info( " -----> Writing chunk %d, size = %.2f M, map time = %.2f ".format( iChunk, chunkMemorySize/1.0E6, (t1-t0)/1.0E9 ) )
//        buffer.put( cacheChunk.data )
//        buffer.force()
//        chunkCache.remove( iChunk )
//        val t2 = System.nanoTime()
//        logger.info( s"Persisted chunk %d, write time = %.2f ".format( iChunk, (t2-t1)/1.0E9 ))
//        runtime.printMemoryUsage(logger)
//      case None =>
//        Thread.sleep( 500 )
//        processChunkFromReader( iChunk, channel )
//    }
//  }
//}

