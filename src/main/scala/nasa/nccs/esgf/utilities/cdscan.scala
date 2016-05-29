package nasa.nccs.esgf.utilities
import java.io.{BufferedWriter, File, FileWriter}

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

  def getNCML( files: Iterator[File] ): xml.Node = {
    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
      <aggregation type="union">
       { for( f <- files ) yield <netcdf location={f.getAbsolutePath}/> }
      </aggregation>
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

