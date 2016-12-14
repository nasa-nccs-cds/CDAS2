package nasa.nccs.cdapi.data

import nasa.nccs.caching.Partition
import nasa.nccs.cdapi.tensors._
import nasa.nccs.cdas.workers.TransVar
import nasa.nccs.esgf.process.CDSection
import nasa.nccs.utilities.{Loggable, cdsutils}
import org.apache.spark.rdd.RDD
import ucar.nc2.constants.AxisType
import ucar.ma2
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

// Developer API for integrating various data management and IO frameworks such as SIA-IO and CDAS-Cache.
// It is intended to be deployed on the master node of the analytics server (this is not a client API).

object MetadataOps {
  def mergeMetadata(opName: String)( metadata0: Map[String, String], metadata1: Map[String, String]): Map[String, String] = {
    metadata0.map { case (key, value) =>
      metadata1.get(key) match {
        case None => (key, value)
        case Some(value1) =>
          if (value == value1) (key, value)
          else (key, opName + "(" + value + "," + value1 + ")")
      }
    }
  }
  def mergeMetadata( opName: String, metadata: Iterable[Map[String, String]] ): Map[String, String] = {
    metadata.foldLeft( metadata.head )( mergeMetadata(opName) )
  }
}

abstract class MetadataCarrier( val metadata: Map[String,String] = Map.empty ) extends Serializable {
  def mergeMetadata( opName: String, other: MetadataCarrier ): Map[String,String] = MetadataOps.mergeMetadata( opName )( metadata, other.metadata )
  def toXml: xml.Elem
  def attr(id:String): String = metadata.getOrElse(id,"")

  implicit def pimp(elem:xml.Elem) = new {
    def %(attrs:Map[String,String]) = {
      val seq = for( (n:String,v:String) <- attrs ) yield new xml.UnprefixedAttribute(n,v,xml.Null)
      (elem /: seq) ( _ % _ )
    }
  }

}

trait RDDataManager {

  def getDatasets(): Set[String]
  def getDatasetMetadata( dsid: String ): Map[String,String]

  def getVariables( dsid: String ): Set[String]
  def getVariableMetadata( vid: String ): Map[String,String]

  def getDataProducts(): Set[String] = Set.empty
  def getDataProductMetadata( pid: String ): Map[String,String] = Map.empty

  def getDataRDD( id: String, domain: Map[AxisType,(Int,Int)] ): RDD[RDDPartition]


}

abstract class ArrayBase[T <: AnyVal]( val shape: Array[Int]=Array.emptyIntArray, val origin: Array[Int]=Array.emptyIntArray, val missing: Option[T]=None, metadata: Map[String,String]=Map.empty, val indexMaps: List[CDCoordMap] = List.empty ) extends MetadataCarrier(metadata) with Serializable {
  def data:  Array[T]
  def toCDFloatArray: CDFloatArray
  def toCDDoubleArray: CDDoubleArray
  def toUcarFloatArray: ucar.ma2.Array = toCDFloatArray
  def toUcarDoubleArray: ucar.ma2.Array = toCDDoubleArray
  def toCDWeightsArray: Option[CDFloatArray] = None
  def getMetadataStr = metadata map { case ( key, value ) => key + ":" + value } mkString (";")
  def merge( other: ArrayBase[T] ): ArrayBase[T]
  def getSampleData( size: Int, start: Int): Array[Float] = toCDFloatArray.getSampleData( size, start )
  def getSampleDataStr( size: Int, start: Int): String = toCDFloatArray.getSampleData( size, start ).mkString( "[ ",", "," ]")
  def combine( combineOp: CDArray.ReduceOp[T], other: ArrayBase[T] ): ArrayBase[T]
  def uid: String = metadata.getOrElse("uid", metadata.getOrElse("collection","") + ":" + metadata.getOrElse("name",""))
  override def toString = "<array shape=(%s), %s> %s </array>".format( shape.mkString(","), metadata.mkString(","), cdsutils.toString(data.mkString(",")) )
}

class HeapFltArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, private val _data:  Array[Float]=Array.emptyFloatArray, _missing: Option[Float]=None,
                    metadata: Map[String,String]=Map.empty, private val _optWeights: Option[Array[Float]]=None, indexMaps: List[CDCoordMap] = List.empty ) extends ArrayBase[Float](shape,origin,_missing,metadata,indexMaps) {
  def data: Array[Float] = _data
  def weights: Option[Array[Float]] = _optWeights
  override def toCDWeightsArray: Option[CDFloatArray] = _optWeights.map( CDFloatArray( shape, _, missing() ) )
  def missing( default: Float = Float.MaxValue ): Float = _missing.getOrElse(default)

  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data, missing(), indexMaps )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data.map(_.toDouble), missing() )

  def merge( other: ArrayBase[Float] ): ArrayBase[Float] = HeapFltArray( toCDFloatArray.merge( other.toCDFloatArray ), origin, mergeMetadata("merge",other), toCDWeightsArray.map( _.merge( other.toCDWeightsArray.get ) ) )
  def combine( combineOp: CDArray.ReduceOp[Float], other: ArrayBase[Float] ): ArrayBase[Float] = HeapFltArray( CDFloatArray.combine( combineOp, toCDFloatArray, other.toCDFloatArray ), origin, mergeMetadata("merge",other), toCDWeightsArray.map( _.merge( other.toCDWeightsArray.get ) ) )
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={missing().toString}> {_data.mkString(",")} </array> % metadata
}
object HeapFltArray {
  def apply( cdarray: CDFloatArray, origin: Array[Int], metadata: Map[String,String], optWeights: Option[CDFloatArray] ): HeapFltArray =
    new HeapFltArray( cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), metadata, optWeights.map( _.getArrayData()), cdarray.getCoordMaps )

  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapFltArray = HeapFltArray( CDArray(ucarray,missing), origin, metadata, None )

  def apply( tvar: TransVar, invalidOpt: Option[Float] ): HeapFltArray = {
    val ucarray: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, tvar.getShape, tvar.getDataBuffer() )
    val floatArray: CDFloatArray = CDFloatArray.cdArrayConverter(CDArray[Float](ucarray, Float.MaxValue ) )
    new HeapFltArray( tvar.getShape, tvar.getOrigin, floatArray.getStorageArray, invalidOpt, tvar.getMetaData.asScala.toMap )
  }
  def empty(rank:Int) = HeapFltArray( CDFloatArray.empty, Array.fill(rank)(0), Map.empty[String,String], None )
}

class HeapDblArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, val _data_ : Array[Double]=Array.emptyDoubleArray, _missing: Option[Double]=None, metadata: Map[String,String]=Map.empty ) extends ArrayBase[Double](shape,origin,_missing,metadata) {
  def data: Array[Double] = _data_
  def missing( default: Double = Double.MaxValue ): Double = _missing.getOrElse(default)
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data.map(_.toFloat), missing().toFloat )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data, missing() )

  def merge( other: ArrayBase[Double] ): ArrayBase[Double] = HeapDblArray( toCDDoubleArray.merge( other.toCDDoubleArray ), origin, mergeMetadata("merge",other) )
  def combine( combineOp: CDArray.ReduceOp[Double], other: ArrayBase[Double] ): ArrayBase[Double] = HeapDblArray( CDDoubleArray.combine( combineOp, toCDDoubleArray, other.toCDDoubleArray ), origin, mergeMetadata("merge",other) )
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={missing().toString} > {_data_.mkString(",")} </array> % metadata
}
object HeapDblArray {
  def apply( cdarray: CDDoubleArray, origin: Array[Int], metadata: Map[String,String] ): HeapDblArray = new HeapDblArray( cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), metadata )
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapDblArray = HeapDblArray( CDDoubleArray.factory(ucarray,missing), origin, metadata )
}

class RDDPartition( val iPart: Int, val elements: Map[String,ArrayBase[Float]] , metadata: Map[String,String] ) extends MetadataCarrier(metadata) {
  def ++( other: RDDPartition ): RDDPartition = {
    assert( (iPart==other.iPart) || (iPart == -1) || (other.iPart == -1), "Attempt to merge RDDPartitions with incommensurate partition indices: %d vs %d".format(iPart,other.iPart ) )
    new RDDPartition( if( iPart >= 0 ) iPart else other.iPart, elements ++ other.elements, metadata ++ other.metadata)
  }
  def element( id: String ): Option[ArrayBase[Float]] = ( elements find { case (key,array) => key.split(':')(0).equals(id) } ) map ( _._2 )
  def findElements( id: String ): Iterable[ArrayBase[Float]] = ( elements filter { case (key,array) => key.split(':')(0).equals(id) } ) values
  def empty( id: String ) = { element(id).isEmpty }
  def head: ( String, ArrayBase[Float] ) = elements.head
  def toXml: xml.Elem = {
    val values: Iterable[xml.Node] = elements.values.map(_.toXml)
    <partition> {values} </partition>  % metadata
  }
}


object RDDPartition {
  def apply ( iPart: Int = -1, elements: Map[String,ArrayBase[Float]] = Map.empty,  metadata: Map[String,String] = Map.empty ) = new RDDPartition( iPart, elements, metadata )
  def apply ( iPart: Int, rdd: RDDPartition ) = new RDDPartition( iPart, rdd.elements, rdd.metadata )
  def merge( rdd_parts: Seq[RDDPartition] ) = rdd_parts.foldLeft( RDDPartition() )( _ ++ _ )
}

object RDDPartSpec {
  def apply( partition: Partition, varSpecs: List[ RDDVariableSpec ] ): RDDPartSpec = new RDDPartSpec( partition, varSpecs )
}

class RDDPartSpec( val partition: Partition, val varSpecs: List[ RDDVariableSpec ] ) extends Serializable with Loggable {
  logger.info( "RDDPartSpec: partition = " + partition.toString + ", varSpecs = " + varSpecs.map(_.toString ).mkString(",") )
  def getRDDPartition: RDDPartition = {
    val elements =  Map( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toHeapArray(partition)) ): _* )
    RDDPartition( partition.index, elements, Map.empty )
  }
  def empty( uid: String ): Boolean = varSpecs.find( _.uid == uid ) match {
    case Some( varSpec ) => varSpec.empty
    case None => true
  }

}

class RDDVariableSpec( val uid: String, val metadata: Map[String,String], val missing: Float, val section: CDSection  ) extends Serializable with Loggable {
  override def toString = s"RDDVariableSpec($uid)[" + section.toString + "]"
  def toHeapArray( partition: Partition ) = {
    logger.info( "toHeapArray: %s, part[%d]: dim=%d, range=(%d:%d), shape=[%s]".format( section.toString(), partition.index, partition.dimIndex, partition.startIndex, partition.endIndex, partition.shape.mkString(",") ) )
    HeapFltArray( partition.dataSection(section, missing), section.getOrigin, metadata, None )
  }
  def empty = section.getShape.contains(0)
  def rank = section.getShape.length
}

