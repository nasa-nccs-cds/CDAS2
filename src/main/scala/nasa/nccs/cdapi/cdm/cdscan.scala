package nasa.nccs.cdapi.cdm

import java.io._
import java.net.{URI, URL}
import java.nio._
import java.nio.file.{FileSystems, Path, Paths}
import java.util.Formatter

import nasa.nccs.cdapi.tensors.CDDoubleArray
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.cds2.utilities.runtime
import nasa.nccs.utilities.Loggable
import nasa.nccs.utilities.cdsutils
import ucar.{ma2, nc2}
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset._
import ucar.nc2.time.CalendarDate

import collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.xml.XML

object NCMLWriter extends Loggable {

  def apply( path: File ): NCMLWriter = { new NCMLWriter( Array(path).iterator ) }

  def isNcFileName( fName: String ): Boolean = { val fname = fName.toLowerCase; fname.endsWith(".nc4") || fname.endsWith(".nc") || fname.endsWith(".hdf") }

  def isNcFile( file: File ): Boolean = { file.isFile && isNcFileName( file.getName.toLowerCase ) }

  def isCollectionFile( file: File ): Boolean = {
    val fname = file.getName.toLowerCase
    file.isFile && fname.endsWith(".csv")
  }
  def getCacheDir: String = {
    val collection_file_path = Collections.getCacheFilePath("local_collections.xml")
    new java.io.File( collection_file_path ).getParent.stripSuffix("/")
  }

  def getCachePath( subdir: String ): Path = {  FileSystems.getDefault().getPath( getCacheDir, subdir ) }

  def getNcURIs(file: File): Iterable[URI] = {
      if( isCollectionFile(file) ) {
        val bufferedSource = Source.fromFile(file)
        val entries = for (line <- bufferedSource.getLines; if isNcFileName(line)) yield new URI(line)
        entries.toIterable
      } else {
        getNcFiles(file).map( _.toURI )
      }
  }

  def getNcFiles(file: File): Iterable[File] = {
    try {
      if (isNcFile(file)) {
        Seq( file )
      } else  {
        val children = new Iterable[File] {
          def iterator = if (file.isDirectory) file.listFiles.iterator else Iterator.empty
        }
        (Seq(file) ++: children.flatMap(getNcFiles(_))).filter(NCMLWriter.isNcFile(_))
      }
    } catch {
      case err: NullPointerException =>
        logger.warn("Empty collection directory: " + file.toString)
        Iterable.empty[File]
    }
  }

  def getNcFiles(args: Iterator[File]): Iterator[File] = args.map( (arg: File) => NCMLWriter.getNcFiles(arg)).flatten
  def getNcURIs(args: Iterator[File]): Iterator[URI] = args.map( (arg: File) => NCMLWriter.getNcURIs(arg)).flatten

  def getFileHeaders( files: IndexedSeq[URI], nReadProcessors: Int ): IndexedSeq[FileHeader] = {
    if( files.length > 0 ) {
      val groupSize = cdsutils.ceilDiv(files.length, nReadProcessors)
      logger.info(" Processing %d files with %d files/group with %d processors".format(files.length, groupSize, nReadProcessors))
      val fileGroups = files.grouped(groupSize).toIndexedSeq
      val fileHeaderFuts = Future.sequence(for (workerIndex <- fileGroups.indices; fileGroup = fileGroups(workerIndex)) yield Future {
        FileHeader.factory(fileGroup, workerIndex)
      })
      Await.result(fileHeaderFuts, Duration.Inf).flatten sortWith { (afr0, afr1) => (afr0.startValue < afr1.startValue) }
    } else IndexedSeq.empty[FileHeader]
  }
}

//class NCMLSerialWriter(val args: Iterator[String]) {
//  val files: IndexedSeq[File] = NCMLWriter.getNcFiles(args).toIndexedSeq
//  val nFiles = files.length
//  val fileHeaders = NCMLWriter.getFileHeadersSerial(files)
//
//  def getNCML: xml.Node = {
//    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
//      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
//      <aggregation dimName="time" units={cdsutils.baseTimeUnits} type="joinExisting">
//        { for( fileHeader <- fileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
//      </aggregation>
//    </netcdf>
//  }
//}

class NCMLWriter(args: Iterator[File], val maxCores: Int = 8) extends Loggable {
  private val nReadProcessors = Math.min(Runtime.getRuntime.availableProcessors, maxCores)
  private val files: IndexedSeq[URI] = NCMLWriter.getNcURIs(args).toIndexedSeq
  assert( !files.isEmpty, "Error, empty collection at: " + args.map( _.getAbsolutePath ).mkString(",") )
  private val nFiles = files.length
  val fileHeaders = NCMLWriter.getFileHeaders(files, nReadProcessors)
  val fileMetadata = FileMetadata(files.head)
  val outerDimensionSize: Int = fileHeaders.foldLeft(0)(_ + _.nElem)
  val ignored_attributes = List( "comments" )

  def isIgnored( attribute: nc2.Attribute ): Boolean = { ignored_attributes.contains(attribute.getShortName) }

  def getAttribute(attribute: nc2.Attribute): xml.Node =
    if (attribute.getDataType == ma2.DataType.STRING) {
      if (attribute.getLength > 1) {
        val sarray: IndexedSeq[String] = (0 until attribute.getLength).map(i => attribute.getStringValue(i).filter(ch => org.jdom2.Verifier.isXMLCharacter(ch)))
          <attribute name={attribute.getShortName} value={sarray.mkString("|")} separator="|"/>
      } else {
          <attribute name={attribute.getShortName} value={attribute.getStringValue(0)}/>
      }
    } else {
      if (attribute.getLength > 1) {
        val sarray: IndexedSeq[String] = (0 until attribute.getLength).map(i => attribute.getNumericValue(i).toString)
          <attribute name={attribute.getShortName} type={attribute.getDataType.toString} value={sarray.mkString(" ")}/>
      } else {
          <attribute name={attribute.getShortName} type={attribute.getDataType.toString} value={attribute.getNumericValue(0).toString}/>
      }
    }

  def getDims(variable: nc2.Variable): String = variable.getDimensions.map(dim => if (dim.isShared) dim.getShortName else if (dim.isVariableLength) "*" else dim.getLength.toString).toArray.mkString(" ")

  def getDimension(axis: CoordinateAxis ): Option[xml.Node] = {
    axis match {
      case coordAxis: CoordinateAxis1D =>
        val nElems = if( coordAxis.getAxisType == AxisType.Time ) outerDimensionSize else coordAxis.getSize
        val dimension = coordAxis.getDimension(0)
          val node = <dimension name={dimension.getFullName} length={nElems.toString} isUnlimited={dimension.isUnlimited.toString} isVariableLength={dimension.isVariableLength.toString} isShared={dimension.isShared.toString}/>
        Some(node)
      case x =>
        logger.warn( "Multidimensional coord axes not currently supported: " + x.getClass.getName + " for axis " + axis.getNameAndDimensions(true) )
        None
    }
  }

  def getAggDataset(fileHeader: FileHeader, timeRegular: Boolean ): xml.Node =
    if( timeRegular ) <netcdf location={fileHeader.filePath} ncoords={fileHeader.nElem.toString}/>
    else <netcdf location={fileHeader.filePath} ncoords={fileHeader.nElem.toString} coordValue={fileHeader.axisValues.map("%.4f".format(_)).mkString(", ")}/>


  def getVariable( variable: nc2.Variable, timeRegularSpecs: Option[(Double,Double)] ): xml.Node = {
    val axisType = fileMetadata.getAxisType(variable)
    <variable name={variable.getFullName} shape={getDims(variable)} type={variable.getDataType.toString}>
      { if( axisType == AxisType.Time ) <attribute name="_CoordinateAxisType" value="Time"/> else for (attribute <- variable.getAttributes; if( !isIgnored( attribute ) ) ) yield getAttribute(attribute) }
      { if( axisType == AxisType.Time ) timeRegularSpecs match { case None => Unit; case Some((t0,dt)) => <values start={"%.3f".format(t0)} increment={"%.6f".format(dt)}/> } }
      { if( (axisType != AxisType.Time) && (axisType != AxisType.RunTime) ) variable match {
        case coordVar: CoordinateAxis1D => getData(variable, coordVar.isRegular)
        case _ => getData(variable, false)
      }}
    </variable>
  }

  def getData(variable: nc2.Variable, isRegular: Boolean): xml.Node = {
    val dataArray: Array[Double] = CDDoubleArray.factory(variable.read).getArrayData()
    if (isRegular) {
      <values start={"%.3f".format(dataArray(0))} increment={"%.6f".format(dataArray(1)-dataArray(0))}/>
    } else {
      <values>
        {dataArray.map(dv => "%.3f".format(dv)).mkString(" ")}
      </values>
    }
  }

  def getTimeSpecs: Option[(Double,Double)] = {
    val t0 = fileHeaders.head.startValue
    val dt = if(fileHeaders.head.nElem > 1) { fileHeaders.head.axisValues(1)-fileHeaders.head.axisValues(0) } else { fileHeaders(1).startValue - fileHeaders(0).startValue }
    Some( t0 -> dt )
  }

  def getAggregation(timeRegular: Boolean ): xml.Node = {
    <aggregation dimName="time" units={cdsutils.baseTimeUnits} type="joinExisting">
      { for( fileHeader <- fileHeaders ) yield { getAggDataset(fileHeader,timeRegular) } }
    </aggregation>
  }

  def getNCML: xml.Node = {
    val timeRegularSpecs= None // getTimeSpecs
    println("Processing %d files with %d workers".format( nFiles, nReadProcessors) )
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <explicit/>
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>

      { for( attribute <- fileMetadata.attributes ) yield getAttribute(attribute) }
      { (for (coordAxis <- fileMetadata.coordinateAxes) yield getDimension(coordAxis)).flatten }
      { for (variable <- fileMetadata.variables) yield getVariable( variable, timeRegularSpecs ) }
      { getAggregation( timeRegularSpecs.isDefined ) }

    </netcdf>
  }

  def writeNCML( ncmlFile: File ) = {
    val bw = new BufferedWriter(new FileWriter( ncmlFile ))
    bw.write( getNCML.toString )
    bw.close()
  }
}

object FileHeader extends Loggable {
  val maxOpenAttempts = 1
  val retryIntervalSecs = 10
  def apply( file: URI, timeRegular: Boolean ): FileHeader = {
    val axisValues: Array[Double] = FileHeader.getTimeCoordValues(file)
    new FileHeader( file.toString, axisValues, timeRegular )
  }

  def factory( files: IndexedSeq[URI], workerIndex: Int ): IndexedSeq[FileHeader] = {
    var retryFiles = new ListBuffer[URI]()
    val timeRegular = false // getTimeAxisRegularity( files.head )
    val firstPass = for (iFile <- files.indices; file = files(iFile)) yield {
      try {
        val t0 = System.nanoTime()
        val fileHeader = FileHeader( file, timeRegular )
        val t1 = System.nanoTime()
        println("Worker[%d]: Processing file[%d] '%s', start = %.3f, ncoords = %d, time = %.4f ".format(workerIndex, iFile, file, fileHeader.startValue, fileHeader.nElem, (t1 - t0) / 1.0E9))
        if( (iFile % 5) == 0 ) runtime.printMemoryUsage(logger)
        Some(fileHeader)
      } catch { case err: Exception =>  retryFiles += file; None }
    }
    val secondPass = for (iFile <- retryFiles.indices; file = retryFiles(iFile)) yield {
      println("Worker[%d]: Reprocessing file[%d] '%s'".format(workerIndex, iFile, file ))
      FileHeader(file,timeRegular)
    }
    firstPass.flatten ++ secondPass
  }

  def getTimeAxisRegularity( ncFile: URI ): Boolean = {
    val ncDataset: NetcdfDataset = openNetCDFFile( ncFile )
    try {
      Option(ncDataset.findCoordinateAxis(AxisType.Time)) match {
        case Some(coordAxis) => coordAxis match {
          case coordAxis: CoordinateAxis1D => coordAxis.isRegular
          case _ => throw new Exception("Multidimensional coord axes not currently supported")
        }
        case None => throw new Exception("ncFile does not have a time axis: " + ncFile )
      }
    } finally { ncDataset.close() }
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

  def openNetCDFFile(ncFile: URI, attempt: Int = 0): NetcdfDataset = try {
    logger.info( "Opening NetCDF dataset at: " + ncFile )
    NetcdfDataset.openDataset( ncFile.toString )
  } catch {
    case ex: Throwable =>
      if (attempt == maxOpenAttempts) throw new Exception("Error opening file '%s' after %d attempts (will retry later): '%s'".format(ncFile, maxOpenAttempts, ex.toString))
      else {
        Thread.sleep( retryIntervalSecs * 1000 )
        openNetCDFFile(ncFile, attempt + 1)
      }
  }

  def getTimeCoordValues(ncFile: URI): Array[Double] = {
    val ncDataset: NetcdfDataset = openNetCDFFile( ncFile )
    Option( ncDataset.findCoordinateAxis( AxisType.Time ) ) match {
      case Some( timeAxis ) =>
        val values = getTimeValues( ncDataset, timeAxis )
        ncDataset.close()
        values
      case None => throw new Exception( "ncFile does not have a time axis: " + ncFile.getRawPath )
    }
  }
}

class DatasetFileHeaders( val aggDim: String, val aggFileMap: Seq[FileHeader] ) {
  def getNElems(): Int = {
    assert( !aggFileMap.isEmpty, "Error, aggregated dataset has no files!" )
    return aggFileMap.head.nElem
  }
  def getAggAxisValues: Array[Double] =
    aggFileMap.foldLeft(Array[Double]()) { _ ++ _.axisValues }
}

class FileHeader( val filePath: String, val axisValues: Array[Double], val timeRegular: Boolean ) {
  def nElem = axisValues.length
  def startValue: Double = axisValues.headOption.getOrElse( Double.NaN )
  override def toString: String = " *** FileHeader { path='%s', nElem=%d, startValue=%f } ".format( filePath, nElem, startValue )
}

object FileMetadata {
  def apply( file: URI ): FileMetadata = new FileMetadata(file)
}

class FileMetadata( val ncFile: URI ) {
  private val ncDataset: NetcdfDataset = FileHeader.openNetCDFFile( ncFile )
  val coordinateAxes = ncDataset.getCoordinateAxes.toList
  val dimensions: List[nc2.Dimension] = ncDataset.getDimensions.toList
  val variables = ncDataset.getVariables.filterNot( v => (v.isCoordinateVariable || v.isMetadata) ).toList
  val coordVars = ncDataset.getVariables.filter( _.isCoordinateVariable ).toList
  val attributes = ncDataset.getGlobalAttributes
  val dimNames = dimensions.map( _.getFullName )
  def getCoordinateAxis( fullName: String ): Option[nc2.dataset.CoordinateAxis] = coordinateAxes.find( p => p.getFullName.equalsIgnoreCase(fullName) )

  def getAxisType( variable: nc2.Variable ): AxisType = variable match {
    case coordVar: CoordinateAxis1D => coordVar.getAxisType;
    case _ => AxisType.RunTime
  }
}
