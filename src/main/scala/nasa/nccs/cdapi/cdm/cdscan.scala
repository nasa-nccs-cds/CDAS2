package nasa.nccs.cdapi.cdm

import java.io._
import java.nio.file.{Path, Paths}
import java.util.Formatter
import java.util.concurrent.ArrayBlockingQueue
import nasa.nccs.utilities.Loggable
import nasa.nccs.utilities.cdsutils
import ucar.{ma2, nc2}
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis1DTime, NetcdfDataset, VariableDS}
import ucar.nc2.ncml.{Aggregation, AggregationExisting, NcMLWriter}
import ucar.nc2.time.CalendarDate
import ucar.nc2.util.{CancelTask, CancelTaskImpl, DiskCache2}

import scala.collection.JavaConversions._
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.xml.XML

object NCMLWriter {

  def isNcFile( file: File ): Boolean = {
    val fname = file.getName.toLowerCase
    file.isFile && (fname.endsWith(".nc4") || fname.endsWith(".nc") || fname.endsWith(".hdf") )
  }

  def getNcFiles(file: File): Iterable[File] = {
    val children = new Iterable[File] {
      def iterator = if (file.isDirectory) file.listFiles.iterator else Iterator.empty
    }
    ( Seq(file) ++: children.flatMap(getNcFiles(_)) ).filter( NCMLWriter.isNcFile(_) )
  }

  def getNcFiles(args: Iterator[String]): Iterator[File] =
    args.map( (arg: String) => NCMLWriter.getNcFiles(new File(arg))).foldLeft(Iterator[File]())(_ ++ _)

  def getFileHeadersSerial( files: IndexedSeq[File] ): IndexedSeq[FileHeader] = {
    println( "NCMLSerialWriter--> Processing %d files:".format(files.length) )
    val fileHeaders: IndexedSeq[FileHeader] = for ( iFile <- files.indices; file = files(iFile) ) yield {
      val fileHeader = FileHeader(file)
      println("  >> Processing file[%d] '%s', start = %d, ncoords = %d ".format(iFile, file.getAbsolutePath, fileHeader.startValue, fileHeader.nElem))
      cdsutils.printHeapUsage
      fileHeader
    }
    fileHeaders.sortWith((afr0, afr1) => (afr0.startValue < afr1.startValue))
  }

  def getFileHeaders( files: IndexedSeq[File], nReadProcessors: Int ): IndexedSeq[FileHeader] = {
    val groupSize = cdsutils.ceilDiv( files.length, nReadProcessors )
    val fileGroups = files.grouped(groupSize).toIndexedSeq
    val fileHeaderFuts  = Future.sequence( for( workerIndex <- fileGroups.indices; fileGroup = fileGroups(workerIndex) ) yield Future { FileHeader.factory( fileGroup, workerIndex ) } )
    Await.result( fileHeaderFuts, Duration.Inf ).foldLeft( IndexedSeq[FileHeader]() ) {_ ++ _} sortWith { ( afr0, afr1 ) =>  (afr0.startValue < afr1.startValue) }
  }
}

class NCMLSerialWriter(val args: Iterator[String]) {
  val files: IndexedSeq[File] = NCMLWriter.getNcFiles(args).toIndexedSeq
  val nFiles = files.length
  val fileHeaders = NCMLWriter.getFileHeadersSerial(files)

  def getNCML: xml.Node = {
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation dimName="time" units="days since 1970-1-1" type="joinExisting">
        { for( fileHeader <- fileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
      </aggregation>
    </netcdf>
  }
}

class NCMLWriter(args: Iterator[String], val maxCores: Int = 20) {
  private val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors - 1, maxCores )
  private val files: IndexedSeq[File]  = NCMLWriter.getNcFiles( args ).toIndexedSeq
  private val nFiles = files.length
  val ncmlWriter = new NcMLWriter()
  val fileHeaders = NCMLWriter.getFileHeaders( files, nReadProcessors )
  val fileMetadata = FileMetadata(files.head)

  def getAttribute( attribute: nc2.Attribute ): xml.Node =
    if( attribute.getDataType == ma2.DataType.STRING ) {
      if( attribute.getLength > 1 ) {
        val sarray: IndexedSeq[String] = (0 until attribute.getLength).map(i => attribute.getStringValue(i).filter(ch => org.jdom2.Verifier.isXMLCharacter(ch)))
          <attribute name={attribute.getShortName} value={sarray.mkString("|")} separator="|"/>
      } else {
          <attribute name={attribute.getShortName} value={attribute.getStringValue(0)} />
      }
    } else {
      if( attribute.getLength > 1 ) {
          val sarray: IndexedSeq[String] = (0 until attribute.getLength).map( i => attribute.getNumericValue(i).toString )
          <attribute name={attribute.getShortName} type={attribute.getDataType.toString} value={sarray.mkString(" ")}/>
      } else {
          <attribute name={attribute.getShortName} type={attribute.getDataType.toString} value={attribute.getNumericValue(0).toString}/>
      }
    }

  def getDims( variable: nc2.Variable ): String = variable.getDimensions.map( dim => if (dim.isShared) dim.getShortName else if (dim.isVariableLength) "*" else dim.getLength.toString ).toArray.mkString(" ")
  def getDimension( dimension: nc2.Dimension ): xml.Node = <dimension name={dimension.getFullName} length={dimension.getLength.toString} isUnlimited={dimension.isUnlimited.toString} isVariableLength={dimension.isVariableLength.toString}/>
  def getAggDataset( fileHeader: FileHeader ): xml.Node = <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString} coordValue={ fileHeader.axisValues.mkString(", ") }/>

  def getNCML: xml.Node = {
    println("Processing %d files with %d workers".format( nFiles, nReadProcessors) )
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      { for( attribute <- fileMetadata.attributes ) yield getAttribute(attribute) }
      { for (dimension <- fileMetadata.dimensions) yield getDimension(dimension) }
      { for (variable <- fileMetadata.variables) yield {
        <variable name={variable.getShortName} shape={getDims(variable)} type={variable.getDataType.toString}>
        { for (attribute <- variable.getAttributes) yield getAttribute(attribute) }
        </variable>
        }
      }
      <aggregation dimName="time" units="days since 1970-1-1" type="joinExisting">
        { for( fileHeader <- fileHeaders ) yield { getAggDataset(fileHeader) } }
      </aggregation>
    </netcdf>
  }
}

object FileHeader extends Loggable {
  val maxOpenAttempts = 10
  val retryIntervalSecs = 2
  def apply( file: File ): FileHeader = new FileHeader( file.getAbsolutePath, FileHeader.getTimeCoordValues(file) )

  def factory( files: IndexedSeq[File], workerIndex: Int ): IndexedSeq[FileHeader] =
    for( iFile <- files.indices; file = files(iFile) ) yield {
      val t0 = System.nanoTime()
      val fileHeader = FileHeader(file)
      val t1 = System.nanoTime()
      println("Worker[%d]: Processing file[%d] '%s', start = %.3f, ncoords = %d, time = %.4f ".format( workerIndex, iFile, file.getAbsolutePath, fileHeader.startValue, fileHeader.nElem, (t1-t0)/1.0E9 ) )
      fileHeader
    }
  def getTimeValues( ncDataset: NetcdfDataset, coordAxis: VariableDS, start_index : Int = 0, end_index : Int = -1, stride: Int = 1 ): Array[Double] = {
    val sec_in_day = 60 * 60 * 24
    val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory( ncDataset, coordAxis, new Formatter())
    val timeCalValues: List[CalendarDate] = timeAxis.getCalendarDates.toList
    val timeZero = CalendarDate.of(timeCalValues.head.getCalendar, 1970, 1, 1, 1, 1, 1)
    val last_index = if ( end_index >= start_index ) end_index else ( timeCalValues.length - 1 )
    val time_values = for (index <- (start_index to last_index by stride); calVal = timeCalValues(index)) yield (calVal.getDifferenceInMsecs(timeZero)/1000).toDouble/sec_in_day
    time_values.toArray[Double]
  }

  def openNetCDFFile(ncFile: File, attempt: Int = 0): NetcdfDataset = try {
    NetcdfDataset.openDataset("file:" + ncFile.getAbsolutePath)
  } catch {
    case ex: java.io.IOException =>
      if (attempt == maxOpenAttempts) throw new Exception("Error opening file '%s' after %d attempts: '%s'".format(ncFile.getName, maxOpenAttempts, ex.getMessage))
      else {
        Thread.sleep( retryIntervalSecs * 1000 )
        logger.warn( "Error opening file '%s' (retry %d): '%s'".format(ncFile.getName, attempt, ex.getMessage) )
        openNetCDFFile(ncFile, attempt + 1)
      }
  }

  def getTimeCoordValues(ncFile: File): Array[Double] = {
    val ncDataset: NetcdfDataset = NetcdfDataset.openDataset( "file:"+ ncFile.getAbsolutePath )
    Option( ncDataset.findCoordinateAxis( AxisType.Time ) ) match {
      case Some( timeAxis ) =>
        val values = getTimeValues( ncDataset, timeAxis )
        ncDataset.close()
        values
      case None => throw new Exception( "ncFile does not have a time axis: " + ncFile.getAbsolutePath )
    }
  }
}

class FileHeader( val path: String, val axisValues: Array[Double] ) {
  def nElem = axisValues.length
  def startValue = axisValues(0)
  override def toString: String = " *** FileHeader { path='%s', nElem=%d, startValue=%d } ".format( path, nElem, startValue )
}

object FileMetadata {
  def apply(file: File): FileMetadata = new FileMetadata(file)
}

class FileMetadata( val ncFile: File ) {
  private val ncDataset: NetcdfDataset = NetcdfDataset.openDataset( "file:"+ ncFile.getAbsolutePath )
  val coordinateAxes = ncDataset.getCoordinateAxes.toList
  val dimensions: List[nc2.Dimension] = ncDataset.getDimensions.toList
  val variables = ncDataset.getVariables.toList
  val attributes = ncDataset.getGlobalAttributes
}

object cdscan extends App {
  val t0 = System.nanoTime()
  val ofile = args(0)
  val ncmlWriter = new NCMLWriter( args.tail.iterator )
  val ncmlNode = ncmlWriter.getNCML
  val file = new File( ofile )
  val bw = new BufferedWriter(new FileWriter(file))
  val nodeStr = ncmlNode.toString
  bw.write( nodeStr )
  bw.close()
  val t1 = System.nanoTime()
  println( "Writing NcML to file '%s', time = %.4f".format( file.getAbsolutePath, (t1-t0)/1.0E9)  )
}

object NCMLWriterTest extends App {
  val t0 = System.nanoTime()
  val ofile = "/tmp/MERRA300.prod.assim.inst3_3d_asm_Cp.xml"
  val ncmlWriter = new NCMLWriter( Array("/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/").iterator )
  val ncmlNode = ncmlWriter.getNCML
  val file = new File( ofile )
  val bw = new BufferedWriter(new FileWriter(file))
  val t1 = System.nanoTime()
  println( "Writing NcML to file '%s', time = %.4f:".format( file.getAbsolutePath, (t1-t0)/1.0E9) )
  val nodeStr = ncmlNode.toString
  println( nodeStr )
  bw.write( nodeStr )
  bw.close()
}

