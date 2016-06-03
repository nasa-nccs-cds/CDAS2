package nasa.nccs.esgf.utilities
import java.io.{BufferedWriter, File, FileWriter}

import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.NetcdfDataset

object NCMLWriter {

  def getNcFiles(args: Array[String]): Iterator[File] =
    args.map( (arg: String) => NCMLWriter.getNcFiles(new File(arg))).foldLeft(Iterator[File]())(_ ++ _)

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

  def getNTimeCoords(ncFile: File): Int = {
    val ncDataset: NetcdfDataset = NetcdfDataset.openDataset( "file:"+ ncFile.getAbsolutePath )
    Option( ncDataset.findCoordinateAxis( AxisType.Time ) ) match {
      case Some( timeAxis ) =>
        val nc = timeAxis.getSize
        println( "Processing file '%s', ncoords = %d ".format( ncFile.getAbsolutePath, nc ) )
        ncDataset.close()
        nc.toInt
      case None => throw new Exception( "ncFile does not have a time axis: " + ncFile.getAbsolutePath )
    }
  }

  def getNCML( files: Iterator[File] ): xml.Node = {
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation dimName="time" type="joinExisting"> {
        var index = 0
        var nChecks = 10
        var ncoords = -1
        for( f <- files ) yield  {
          if( index < nChecks ) {
            val current_ncoords = getNTimeCoords(f)
            if ( ( ncoords >= 0 ) && ( ncoords != current_ncoords ) ) { nChecks = Int.MaxValue }
            ncoords = current_ncoords
            index = index + 1
          }
            <netcdf location={"file:"+f.getAbsolutePath} ncoords={ncoords.toString}/>
        }
      } </aggregation>
    </netcdf>
  }

}

object cdscan extends App {
  val ofile = args(0)
  val files = NCMLWriter.getNcFiles( args.tail )
  val ncml = NCMLWriter.getNCML( files )
  val file = new File( ofile )
  val bw = new BufferedWriter(new FileWriter(file))
  println( "Writing NcML to file '%s'".format( file.getAbsolutePath ))
  bw.write( ncml.toString )
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

