package nasa.nccs.esgf.utilities
import java.io.{BufferedWriter, File, FileWriter}
import java.util.Formatter
import java.util.concurrent.ArrayBlockingQueue

import nasa.nccs.caching.{Cache, LruCache}
import nasa.nccs.utilities.cdsutils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D, CoordinateAxis1DTime, NetcdfDataset, VariableDS}
import ucar.nc2.time.{CalendarDate, CalendarDateRange}

import scala.concurrent.{Await, Future}

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
}

class NCMLSerialWriter(val args: Iterator[String]) {

  def getFileHeadersSerial(): IndexedSeq[FileHeader] = {
    val files: IndexedSeq[File] = NCMLWriter.getNcFiles(args).toIndexedSeq
    val nFiles = files.length
    println( "NCMLSerialWriter--> Processing %d files:".format(nFiles) )
    val fileHeaders: IndexedSeq[FileHeader] = for ( iFile <- files.indices; file = files(iFile) ) yield {
      val fileHeader = new FileHeader(file)
      println("  >> Processing file[%d] '%s', start = %d, ncoords = %d ".format(iFile, file.getAbsolutePath, fileHeader.startValue, fileHeader.nElem))
      cdsutils.printHeapUsage
      fileHeader
    }
    fileHeaders.sortWith((afr0, afr1) => (afr0.startValue < afr1.startValue))
  }

  def getNCML: xml.Node = {
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation dimName="time" units="seconds since 1970-1-1" type="joinExisting">
        { for( fileHeader <- getFileHeadersSerial ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
      </aggregation>
    </netcdf>
  }
}

class NCMLWriter(args: Iterator[String], val maxCores: Int = 10) {
  private val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors - 1, maxCores )
  private val files: IndexedSeq[File]  = NCMLWriter.getNcFiles( args ).toIndexedSeq
  private val nFiles = files.length
  private val fileHeaderQueue = new ArrayBlockingQueue[FileHeader]( nFiles )

  def getFileHeaders(): IndexedSeq[FileHeader] = {
    val groupSize = Math.ceil(nFiles/nReadProcessors.toFloat).toInt
    val fileHeaderFuts  = Future.sequence( for( fileGroup <- files.sliding(groupSize) ) yield Future { FileHeader.factory(fileGroup) } )
    Await.result( fileHeaderFuts, Duration.Inf ).foldLeft( IndexedSeq[FileHeader]() )(_ ++ _).sortWith( ( afr0, afr1 ) =>  (afr0.startValue < afr1.startValue) )
  }

  def getNCML: xml.Node = {
    println("Processing %d files with %d workers".format( nFiles, nReadProcessors) )
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation dimName="time" units="seconds since 1970-1-1" type="joinExisting">
        { for( fileHeader <- getFileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
      </aggregation>
    </netcdf>
  }
}

object FileHeader {
  def apply( file: File ): FileHeader = new FileHeader(file)
  def factory( files: IndexedSeq[File] ): IndexedSeq[FileHeader] =
    for( iFile <- files.indices; file = files(iFile) ) yield {
      val fileHeader = new FileHeader(file)
      println("Processing file[%d] '%s', start = %d, ncoords = %d ".format( iFile, file.getAbsolutePath, fileHeader.startValue, fileHeader.nElem ) )
      fileHeader
    }
}

class FileHeader( file: File ) {
  val path: String = file.getAbsolutePath
  val axisValues: Array[Long] =  getTimeCoordValues(file)
  def nElem = axisValues.length
  def startValue = axisValues(0)

  override def toString: String = " *** FileHeader { path='%s', nElem=%d, startValue=%d } ".format( path, nElem, startValue )

  def getTimeValues( ncDataset: NetcdfDataset, coordAxis: VariableDS, start_index : Int = 0, end_index : Int = -1, stride: Int = 1 ): Array[Long] = {
    val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory( ncDataset, coordAxis, new Formatter())
    val timeCalValues: List[CalendarDate] = timeAxis.getCalendarDates.toList
    val timeZero = CalendarDate.of(timeCalValues.head.getCalendar, 1970, 1, 1, 1, 1, 1)
    val last_index = if ( end_index >= start_index ) end_index else ( timeCalValues.length - 1 )
    val time_values = for (index <- (start_index to last_index by stride); calVal = timeCalValues(index)) yield calVal.getDifferenceInMsecs(timeZero) / 1000
    time_values.toArray[Long]
  }

  def getTimeCoordValues(ncFile: File): Array[Long] = {
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

