package nasa.nccs.cdapi.cdm

import java.nio.channels.NonReadableChannelException

import nasa.nccs.esgf.process.DomainAxis
import nasa.nccs.cdapi.cdm
import ucar.nc2
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateSystem, NetcdfDataset}

import scala.collection.mutable
import scala.collection.concurrent
import scala.collection.JavaConversions._
// import scala.collection.JavaConverters._

object Collection {
  def apply( ctype: String, url: String, vars: List[String] = List() ) = { new Collection(ctype,url,vars) }
}
class Collection( val ctype: String, val url: String, val vars: List[String] = List() ) {
  def getUri( varName: String = "" ) = {
    ctype match {
      case "dods" => s"$url/$varName.ncml"
      case "file" => url
      case _ => throw new Exception( s"Unrecognized collection type: $ctype")
    }
  }
  override def toString = "Collection( type=%s, url=%s, vars=(%s))".format( ctype, url, vars.mkString(",") )
}

object CDSDataset {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def load( dsetName: String, collection: Collection, varName: String ): CDSDataset = {
    val uri = collection.getUri(varName)
    load(dsetName, uri, varName)
  }

  def load( dsetName: String, uri: String, varName: String ): CDSDataset = {
    val t0 = System.nanoTime
    val ncDataset: NetcdfDataset = loadNetCDFDataSet( uri )
    val coordSystems: List[CoordinateSystem] = ncDataset.getCoordinateSystems.toList
    assert( coordSystems.size <= 1, "Multiple coordinate systems for one dataset is not supported" )
    if(coordSystems.isEmpty) throw new IllegalStateException("Error creating coordinate system for variable " + varName )
    val rv = new CDSDataset( dsetName, uri, ncDataset, coordSystems.head )
    val t1 = System.nanoTime
    logger.info( "loadDataset(%s)T> %.4f,  ".format( uri, (t1-t0)/1.0E9 ) )
    rv
  }

  private def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    logger.info("Opening NetCDF dataset %s".format(url))
    try {
      NetcdfDataset.openDataset(url)
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(url))
        throw e
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(url))
        throw ex
    }
  }
}

class CDSDataset( val name: String, val uri: String, val ncDataset: NetcdfDataset, val coordSystem: CoordinateSystem ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val attributes: List[nc2.Attribute] = ncDataset.getGlobalAttributes.map( a => { new nc2.Attribute( name + "--" + a.getFullName, a ) } ).toList
  val coordAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList

  def getCoordinateAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList

  def loadVariable( varName: String ): cdm.CDSVariable = {
    val t0 = System.nanoTime
    val ncVariable = ncDataset.findVariable(varName)
    if (ncVariable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
    val rv = new cdm.CDSVariable( varName, this, ncVariable )
    val t1 = System.nanoTime
    logger.info( "loadVariable(%s)T> %.4f,  ".format( varName, (t1-t0)/1.0E9 ) )
    rv
  }

  def findCoordinateAxis( fullName: String ): Option[CoordinateAxis] = ncDataset.findCoordinateAxis( fullName ) match { case null => None; case x => Some( x ) }

  def getCoordinateAxis( axisType: DomainAxis.Type.Value ): Option[CoordinateAxis] = {
    axisType match {
      case DomainAxis.Type.X => Option( coordSystem.getXaxis )
      case DomainAxis.Type.Y => Option( coordSystem.getYaxis )
      case DomainAxis.Type.Z => Option( coordSystem.getHeightAxis )
      case DomainAxis.Type.Lon => Option( coordSystem.getLonAxis )
      case DomainAxis.Type.Lat => Option( coordSystem.getLatAxis )
      case DomainAxis.Type.Lev => Option( coordSystem.getPressureAxis )
      case DomainAxis.Type.T => Option( coordSystem.getTaxis )
    }
  }

  def getCoordinateAxis(axisType: Char): CoordinateAxis = {
    axisType.toLower match {
      case 'x' => if (coordSystem.isGeoXY) coordSystem.getXaxis else coordSystem.getLonAxis
      case 'y' => if (coordSystem.isGeoXY) coordSystem.getYaxis else coordSystem.getLatAxis
      case 'z' =>
        if (coordSystem.containsAxisType(AxisType.Pressure)) coordSystem.getPressureAxis
        else if (coordSystem.containsAxisType(AxisType.Height)) coordSystem.getHeightAxis else coordSystem.getZaxis
      case 't' => coordSystem.getTaxis
      case x => throw new Exception("Can't recognize axis type '%c'".format(x))
    }
  }
}

// var.findDimensionIndex(java.lang.String name)
