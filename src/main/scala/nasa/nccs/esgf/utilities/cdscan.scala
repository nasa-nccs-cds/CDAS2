package nasa.nccs.esgf.utilities
import java.io.{BufferedWriter, File, FileWriter}
import java.util.Formatter

import nasa.nccs.caching.{Cache, LruCache}
import nasa.nccs.esgf.process.DomainAxis
import ucar.ma2
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D, CoordinateAxis1DTime, NetcdfDataset, VariableDS}
import ucar.nc2.time.{CalendarDate, CalendarDateRange}

import scala.concurrent.duration.Duration
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

class NCMLWriter(args: Iterator[String]) {
  private val nReadProcessors = Runtime.getRuntime.availableProcessors - 1
  val aggFileRecCache: Cache[Int,AggFileRec] = new LruCache("Store","cdscan",false)
  val ncFiles: IndexedSeq[File]  = NCMLWriter.getNcFiles( args ).toIndexedSeq

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

  def readAggFiles( coreIndex: Int ): Int = {
    var nElementsWritten = 0
    for( iFile <- (coreIndex until ncFiles.length by nReadProcessors); file = ncFiles.get(iFile) ) {
      val values = getTimeCoordValues(file)
      println( "Core[%d]: Processing file '%s', ncoords = %d ".format( coreIndex, file.getAbsolutePath, values.length ) )
      val aggFileRec = new AggFileRec( file.getAbsolutePath, values )
      aggFileRecCache.put( iFile, aggFileRec )
      nElementsWritten += 1
    }
    nElementsWritten
  }

  def getAggFileRec( fileIndex: Int ): AggFileRec = {
    aggFileRecCache.get(fileIndex) match {
      case Some( aggFileRecFut: Future[AggFileRec] ) =>
        Await.result( aggFileRecFut, Duration.Inf )
      case None =>
        Thread.sleep( 200 )
        getAggFileRec( fileIndex )
    }
  }

  def getNCML: xml.Node = {
    val readProcFuts: IndexedSeq[Future[Int]] = for( coreIndex <- (0 until Math.min( ncFiles.length, nReadProcessors ) ) ) yield Future { readAggFiles(coreIndex) }
    val aggFileRecCache: Cache[Int,AggFileRec] = new LruCache("Store","cdscan",false)
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation dimName="time" units="seconds since 1970-1-1" type="joinExisting">
        { for( iFile <- (0 until ncFiles.length ); aggFileRec = getAggFileRec(iFile) ) yield
            <netcdf location={"file:" + aggFileRec.path} ncoords={aggFileRec.nElem.toString}> { aggFileRec.axisValues.mkString(", ") } </netcdf>
        }
      </aggregation>
    </netcdf>
  }
}

case class AggFileRec( val path: String, val axisValues: Array[Long] ) {
  def nElem = axisValues.length
}

object cdscan extends App {
  val ofile = args(0)
  val ncmlWriter = new NCMLWriter( args.tail.iterator )
  val ncmlNode = ncmlWriter.getNCML
  val file = new File( ofile )
  val bw = new BufferedWriter(new FileWriter(file))
  println( "Writing NcML to file '%s'".format( file.getAbsolutePath ))
  val nodeStr = ncmlNode.toString
  bw.write( nodeStr )
  bw.close()
}

//  val file = new File("ncml.xml")
//  val bw = new BufferedWriter(new FileWriter(file))
//  bw.write( ncml.toString )
//  bw.close()



//object test extends App {
//  val file_paths = Array( "/Users/tpmaxwel/Data/MERRA/DAILY" )
//  val files = cdscan.getNcFiles( file_paths )
//  val ncml = cdscan.getNCML( files )
//  printf( ncml.toString )
//}
//

object NCMLWriterTest extends App {
  val ofile = "/tmp/MERRA300.prod.assim.inst3_3d_asm_Cp.xml"
  val ncmlWriter = new NCMLWriter( Array("/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/").iterator )
  val ncmlNode = ncmlWriter.getNCML
  val file = new File( ofile )
  val bw = new BufferedWriter(new FileWriter(file))
  println( "Writing NcML to file '%s':".format( file.getAbsolutePath ))
  val nodeStr = ncmlNode.toString
  println( nodeStr )
  bw.write( nodeStr )
  bw.close()
}

