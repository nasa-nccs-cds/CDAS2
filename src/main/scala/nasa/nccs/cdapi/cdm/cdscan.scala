package nasa.nccs.cdapi.cdm

import java.io._
import java.nio.file.{Paths, Path}
import java.util.Formatter
import java.util.concurrent.ArrayBlockingQueue
import ucar.nc2
import nasa.nccs.utilities.cdsutils
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{NetcdfDataset, CoordinateAxis1DTime, VariableDS}
import ucar.nc2.ncml.{NcMLWriter, Aggregation, AggregationExisting}
import ucar.nc2.time.CalendarDate
import ucar.nc2.util.{CancelTaskImpl, CancelTask, DiskCache2}

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
    val fileHeaderFuts  = Future.sequence( for( fileGroup <- files sliding groupSize ) yield Future { FileHeader factory fileGroup } )
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
      <aggregation dimName="time" units="seconds since 1970-1-1" type="joinExisting">
        { for( fileHeader <- fileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
      </aggregation>
    </netcdf>
  }
}

class NCMLAggregation(val args: Iterator[String]) {
  val files: IndexedSeq[File] = NCMLWriter.getNcFiles(args).toIndexedSeq
  val nFiles = files.length
  val fileHeaders = NCMLWriter.getFileHeadersSerial(files)

  def getAggregation: NetcdfDataset = {
    val aggDataset: NetcdfDataset= new NetcdfDataset()
    val aggregation = new AggregationExisting(aggDataset, "time", "")
    for( fileHeader <- fileHeaders ) {
      val filename = Paths.get(fileHeader.path).getFileName.toString
      val id = filename.substring(0, filename.lastIndexOf('.'))
      aggregation.addExplicitDataset( "test", fileHeader.path, id, fileHeader.nElem.toString, fileHeader.axisValues.mkString(","), null, null )
    }
    aggDataset.setAggregation(aggregation)
    val cancelTask = new CancelTaskImpl()
    aggregation.finish( cancelTask )
    aggDataset
  }

  def getNCML: xml.Node = {
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation dimName="time" units="seconds since 1970-1-1" type="joinExisting">
        { for( fileHeader <- fileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
      </aggregation>
    </netcdf>
  }
}

class NCMLWriter(args: Iterator[String], val maxCores: Int = 10) {
  private val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors - 1, maxCores )
  private val files: IndexedSeq[File]  = NCMLWriter.getNcFiles( args ).toIndexedSeq
  private val nFiles = files.length
  val ncmlWriter = new NcMLWriter()
  val fileHeaders = NCMLWriter.getFileHeaders( files, nReadProcessors )
  val fileMetadata = FileMetadata(files.head)

  def makeAttributeElement(attribute: Nothing): Nothing = {
    val attElem: Nothing = new Nothing("attribute", namespace)
    attElem.setAttribute("name", attribute.getShortName)
    val dt: Nothing = attribute.getDataType
    if ((dt != null) && (dt ne DataType.STRING)) attElem.setAttribute("type", dt.toString)
    if (attribute.getLength eq 0) {
      return attElem
    }
    if (attribute.isString) {
      val buff: Nothing = new Nothing
      {
        var i: Int = 0
        while (i < attribute.getLength) {
          {
            val sval: Nothing = attribute.getStringValue(i)
            if (i > 0) buff.append("|")
            buff.append(sval)
          }
          ({
            i += 1; i - 1
          })
        }
      }
      attElem.setAttribute("value", Parse.cleanCharacterData(buff.toString))
      if (attribute.getLength > 1) attElem.setAttribute("separator", "|")
    }
    else {
      val buff: Nothing = new Nothing
      {
        var i: Int = 0
        while (i < attribute.getLength) {
          {
            val `val`: Nothing = attribute.getNumericValue(i)
            if (i > 0) buff.append(" ")
            buff.append(`val`.toString)
          }
          ({
            i += 1; i - 1
          })
        }
      }
      attElem.setAttribute("value", buff.toString)
    }
    return attElem
  }

  def makeVariableNode( variable: nc2.Variable ) = {
    val buff: Nothing = new Nothing
    val dims: Array[String] = variable.getDimensions.map( dim => if (dim.isVariableLength) "*" else dim.getLength.toString )

    import scala.collection.JavaConversions._
    for (att <- variable.getAttributes) makeAttributeElement(att)
    <variable name={variable.getShortName} shape={dims.mkString(" ")} type={variable.getDataType.toString} >
    </variable>
}
  def getNCML: xml.Node = {
    println("Processing %d files with %d workers".format( nFiles, nReadProcessors) )
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      { for( dim <- fileMetadata.dimensions ) yield { <dimension name={dim.getFullName} length={dim.getLength} isUnlimited={dim.isUnlimited} isVariableLength={dim.isVariableLength} isShared={dim.isShared}/> }
        for( variable <- fileMetadata.variables; varElement = ncmlWriter. .makeVariableElement(variable, false) ) yield {

            <variable name={variable.getShortName} shape={variable.getShape} type={variable.isUnlimited} isVariableLength={dim.isVariableLength} isShared={dim.isShared}/> }
      }
      <aggregation dimName="time" units="seconds since 1970-1-1" type="joinExisting">
        { for( fileHeader <- fileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString} coordValue={ fileHeader.axisValues.mkString(", ") }/> } }
      </aggregation>
    </netcdf>
  }
}

object FileHeader {
  def apply( file: File ): FileHeader = new FileHeader( file.getAbsolutePath, FileHeader.getTimeCoordValues(file) )

  def factory( files: IndexedSeq[File] ): IndexedSeq[FileHeader] =
    for( iFile <- files.indices; file = files(iFile) ) yield {
      val fileHeader = FileHeader(file)
      println("Processing file[%d] '%s', start = %d, ncoords = %d ".format( iFile, file.getAbsolutePath, fileHeader.startValue, fileHeader.nElem ) )
      fileHeader
    }
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

class FileHeader( val path: String, val axisValues: Array[Long] ) {
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
  val dimensions = ncDataset.getDimensions.toList
  val variables = ncDataset.getVariables.toList
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


object AggregationTest0 extends App {
  Aggregation.setPersistenceCache( new DiskCache2(".cdas/ncml",true,60*24*90,-1) )
  val aggDataset: NetcdfDataset= new NetcdfDataset()
  val aggregation = new AggregationExisting(aggDataset, "time", "")
  aggregation.addDatasetScan( null, "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", "nc", null, null, null, "true", null )
  val cacheFile = "/Users/tpmaxwel/.cdas/test.ncml"
  aggDataset.setAggregation( aggregation )
  val ostr = new ObjectOutputStream ( new FileOutputStream( cacheFile ) )
  aggDataset.writeNcML( ostr, null )
}

object AggregationTest1 extends App {
  Aggregation.setPersistenceCache( new DiskCache2(".cdas/ncml",true,60*24*90,-1) )
  val ncmlAggregation = new NCMLAggregation( Array("/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/").iterator )
  val aggDataset = ncmlAggregation.getAggregation
  val cacheFile = "/Users/tpmaxwel/.cdas/test.ncml"
  val ostr = new ObjectOutputStream ( new FileOutputStream( cacheFile ) )
  aggDataset.writeNcML( ostr, null )
}

