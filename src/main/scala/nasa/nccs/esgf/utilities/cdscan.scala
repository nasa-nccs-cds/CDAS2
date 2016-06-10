package nasa.nccs.esgf.utilities
import java.io.{BufferedWriter, File, FileWriter}
import java.util.Formatter
import java.util.concurrent.ArrayBlockingQueue
import nasa.nccs.caching.{Cache, LruCache}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D, CoordinateAxis1DTime, NetcdfDataset, VariableDS}
import ucar.nc2.time.{CalendarDate, CalendarDateRange}
import scala.concurrent.Future

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

class NCMLWriter(args: Iterator[String], val maxCores: Int = 10) {
  private val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors - 1, maxCores )
  private val files: IndexedSeq[File]  = NCMLWriter.getNcFiles( args ).toIndexedSeq
  private val nFiles = files.length
  private val fileQueue: ArrayBlockingQueue[Option[File]] = initFileQueue( files )
  private val aggFileRecQueue = new ArrayBlockingQueue[AggFileRec]( nFiles )

  def initFileQueue( ncFiles: IndexedSeq[File] ): ArrayBlockingQueue[Option[File]] = {
    val queue = new ArrayBlockingQueue[Option[File]]( ncFiles.length + nReadProcessors )
    ncFiles foreach { f => queue.put(Some(f)) }
    0 until nReadProcessors foreach { _ => queue.put(None) }
    queue
  }

  def processFiles(coreIndex: Int): Unit = {
    breakable { for (iFile <- 0 to nFiles) {
        fileQueue.take() match {
          case None => break
          case Some(file) =>
            val aggFileRec = new AggFileRec(file)
            aggFileRecQueue.put(aggFileRec)
            println("Core[%d]: Processing file[%d] '%s', start = %d, ncoords = %d ".format(coreIndex, iFile, file.getAbsolutePath, aggFileRec.startValue, aggFileRec.nElem))
        }
      }
    }
  }

  def getAggFileRecs(): IndexedSeq[AggFileRec] = {
    val readProcFuts: IndexedSeq[Future[Unit]] = for( coreIndex <- (0 until Math.min( nFiles, nReadProcessors ) ) ) yield Future { processFiles(coreIndex) }
    val aggFileRecs = for( iFile <- (0 until nFiles) ) yield aggFileRecQueue.take
    aggFileRecs.sortWith( ( afr0, afr1 ) =>  (afr0.startValue < afr1.startValue) )
  }

  def getNCML: xml.Node = {
    println("Processing %d files with %d workers".format( nFiles, nReadProcessors) )
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation dimName="time" units="seconds since 1970-1-1" type="joinExisting">
        { for( aggFileRec <- getAggFileRecs ) yield { <netcdf location={"file:" + aggFileRec.path} ncoords={aggFileRec.nElem.toString}> { aggFileRec.axisValues.mkString(", ") } </netcdf> } }
      </aggregation>
    </netcdf>
  }
}

case class AggFileRec( file: File ) {
  val path: String = file.getAbsolutePath
  val axisValues: Array[Long] =  getTimeCoordValues(file)
  def nElem = axisValues.length
  def startValue = axisValues(0)

  override def toString: String = " *** AggFileRec { path='%s', nElem=%d, startValue=%d } ".format( path, nElem, startValue )

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

object NCMLWriterTest extends App {
  val t0 = System.nanoTime()
  val ofile = "/tmp/MERRA300.prod.assim.inst3_3d_asm_Cp.xml"
  val ncmlWriter = new NCMLWriter( Array("/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/").iterator )
  val ncmlNode = ncmlWriter.getNCML
  val file = new File( ofile )
  val bw = new BufferedWriter(new FileWriter(file))
  val t1 = System.nanoTime()
  println( "Writing NcML to file '%s', time = %.2f:".format( file.getAbsolutePath, (t1-t0)/1.0E9) )
  val nodeStr = ncmlNode.toString
  println( nodeStr )
  bw.write( nodeStr )
  bw.close()
}

