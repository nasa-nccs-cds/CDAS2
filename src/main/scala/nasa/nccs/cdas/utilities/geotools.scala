package nasa.nccs.cdas.utilities

import com.vividsolutions.jts.geom
import nasa.nccs.cdapi.tensors.CDByteArray
import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import java.nio.ByteBuffer
import ucar.ma2
import scala.collection.mutable.ListBuffer


class GeoTools( val SRID: Int = 4326 ) {
  val precisionModel = new geom.PrecisionModel(geom.PrecisionModel.FLOATING_SINGLE)
  val geometryFactory = new geom.GeometryFactory(precisionModel, SRID)
  val bTrue: Byte = 1
  val bFalse: Byte = 0

  def readShapefile(filePath: String): geom.MultiPolygon = {
    val in = new ShpFiles(filePath)
    val r: ShapefileReader = new ShapefileReader(in, false, false, geometryFactory)
    val polyList = new ListBuffer[geom.Polygon]()
    while (r.hasNext) r.nextRecord.shape match {
      case poly: geom.Polygon => polyList += poly
      case mpoly: geom.MultiPolygon => for (ig <- (0 until mpoly.getNumGeometries); geo = mpoly.getGeometryN(ig)) geo match {
        case poly: geom.Polygon => polyList += poly
      }
    }
    r.close()
    new geom.MultiPolygon(polyList.toArray, geometryFactory)
  }

  def getGrid(bounds: Array[Double], shape: Array[Int], spatial_axis_indices: Array[Int]): geom.MultiPoint = {
    val nx = shape(spatial_axis_indices(0))
    val ny = shape(spatial_axis_indices(1))
    val dx = (bounds(1) - bounds(0)) / nx
    val dy = (bounds(3) - bounds(2)) / ny
    val geoPts: IndexedSeq[geom.Coordinate] = for (ix <- (0 until nx); iy <- (0 until ny); x = bounds(0) + ix * dx; y = bounds(2) + iy * dy) yield new geom.Coordinate(x, y)
    geometryFactory.createMultiPoint(geoPts.toArray)
  }

  def printGridCoords(bounds: Array[Double], shape: Array[Int]): Unit = {
    val dx = (bounds(1) - bounds(0)) / shape(0)
    val dy = (bounds(3) - bounds(2)) / shape(1)
    for (ix <- (0 until shape(0)); x = bounds(0) + ix * dx) {
      val coords = for (iy <- (0 until shape(1)); y = bounds(2) + iy * dy) yield new geom.Coordinate(x, y)
      println(coords.toList.mkString(", "))
    }
  }

  def pointsToMask(grid: geom.MultiPoint, mask_points: geom.MultiPoint): Array[Byte] = {
    val maskPointCoords: Set[geom.Coordinate] = mask_points.getCoordinates.toSet
    val orderedPoints = for (gridCoord <- grid.getCoordinates) yield
      if (maskPointCoords.contains(gridCoord)) {
        bTrue
      } else {
        bFalse
      }
    orderedPoints
  }

  def getMask(mask_polys: geom.MultiPolygon, bounds: Array[Double], shape: Array[Int], spatial_axis_indices: Array[Int]): Array[Byte] =
    getMask(mask_polys, getGrid(bounds, shape, spatial_axis_indices))

  def getMask(mask_polys: geom.MultiPolygon, grid: geom.MultiPoint): Array[Byte] = {
    val intersectedPoints = mask_polys.intersection(grid)
    val mask_buffer: Array[Byte] = intersectedPoints match {
      case mask_mpt: geom.MultiPoint => pointsToMask(grid, mask_mpt)
      case x => throw new Exception("Unexpected result type from grid intersection: " + x.getClass.getCanonicalName)
    }
    mask_buffer
  }

  def getMaskArray(boundary: geom.MultiPolygon, bounds: Array[Double], shape: Array[Int], spatial_axis_indices: Array[Int]): ma2.Array = {
    ma2.Array.factory(ma2.DataType.BYTE, shape, ByteBuffer.wrap(getMask(boundary, bounds, shape, spatial_axis_indices)))
  }

  def testPoint(mask_geom: geom.Geometry, testpoint: Array[Float]): Boolean = {
    val geo_pt = geometryFactory.createPoint(new geom.Coordinate(testpoint(0), testpoint(1)))
    mask_geom.contains(geo_pt)
  }

  def produceMask(shapefile_path: String, bounds: Array[Double], mask_shape: Array[Int], spatial_axis_indices: Array[Int]): CDByteArray = {
    val mask_array = getMask( readShapefile(shapefile_path), bounds, mask_shape, spatial_axis_indices )
    CDByteArray(mask_shape, mask_array)
  }


}

class maskPointsTest {
//  val oceanShapeUrl=getClass.getResource("/shapes/110m/ocean/ne_110m_ocean.shp")
  val oceanShapeUrl = getClass.getResource("/shapes/ocean50m/ne_50m_ocean.shp")
  val geotools = new GeoTools()
  val mask_geom: geom.MultiPolygon = geotools.readShapefile(oceanShapeUrl.getPath())
  for (y <- (-85 to 85 by 10); test_point = Array[Float](20, y)) {
    val test_result = geotools.testPoint(mask_geom, test_point)
    println("Test Point: (%s), mask contains point: %s".format(test_point.mkString(","), test_result.toString))
  }
}

//object maskGridTest extends App {
////  val oceanShapeUrl=getClass.getResource("/shapes/ocean50m/ne_50m_ocean.shp")
//  val oceanShapeUrl=getClass.getResource("/shapes/ocean110m/ne_110m_ocean.shp")
//  val geotools = new GeoTools()
//  val shape = Array(360,180)
//  val t0 = System.nanoTime
//  val mask_geom: geom.MultiPolygon = geotools.readShapefile( oceanShapeUrl.getPath() )
//  val t1 = System.nanoTime
//
//
////  val mask1: Array[Byte]  = geotools.getMask( mask_geom, Array(0f,360f,-89.5f,90.5f), Array(360,180) )
//  val mask2: ma2.Array    = geotools.getMaskArray( mask_geom, Array(-180f,180f,-89.5f,90.5f), shape )
//  val mask_shape = mask2.getShape()
//
//  val t2 = System.nanoTime
//  println( "Mask read time = %.3f, mask compute time = %.3f".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
//  for( iy <-((shape(1)-1) to 0 by -1 ) ) println( new String( mask2.slice(1,iy).getDataAsByteBuffer.array.map( _ match { case 1 => '*'; case 0 => '_'; case x => 'x' } ) ) )
//}
//
