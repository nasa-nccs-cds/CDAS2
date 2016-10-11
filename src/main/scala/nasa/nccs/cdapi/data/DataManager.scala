package nasa.nccs.cdapi.data

import nasa.nccs.caching.Partition
import nasa.nccs.cdapi.tensors.{CDArray, CDDoubleArray, CDFloatArray}
import nasa.nccs.esgf.process.CDSection
import nasa.nccs.utilities.{Loggable, cdsutils}
import org.apache.spark.rdd.RDD
import ucar.nc2.constants.AxisType

// Developer API for integrating various data management and IO frameworks such as SIA-IO and CDAS-Cache.
// It is intended to be deployed on the master node of the analytics server (this is not a client API).

object MetadataOps {
  def mergeMetadata(opName: String, metadata0: Map[String, String], metadata1: Map[String, String]): Map[String, String] = {
    metadata0.map { case (key, value) =>
      metadata1.get(key) match {
        case None => (key, value)
        case Some(value1) =>
          if (value == value1) (key, value)
          else (key, opName + "(" + value + "," + value1 + ")")
      }
    }
  }
}

abstract class MetadataCarrier( val metadata: Map[String,String] = Map.empty ) extends Serializable {
  def mergeMetadata( opName: String, other: MetadataCarrier ): Map[String,String] = MetadataOps.mergeMetadata( opName, metadata, other.metadata )
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

abstract class ArrayBase[T <: AnyVal]( val shape: Array[Int]=Array.emptyIntArray, val origin: Array[Int]=Array.emptyIntArray, val missing: T=null, metadata: Map[String,String]=Map.empty ) extends MetadataCarrier(metadata) with Serializable {
  def data:  Array[T]
  def toCDFloatArray: CDFloatArray
  def toCDDoubleArray: CDDoubleArray
  def toUcarFloatArray: ucar.ma2.Array = toCDFloatArray
  def toUcarDoubleArray: ucar.ma2.Array = toCDDoubleArray
  def merge( other: ArrayBase[T] ): ArrayBase[T]
  def combine( combineOp: CDArray.ReduceOp[T], other: ArrayBase[T] ): ArrayBase[T]
  override def toString = "<array shape=(%s), %s> %s </array>".format( shape.mkString(","), metadata.mkString(","), cdsutils.toString(data.mkString(",")) )
}

class HeapFltArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, private val _data:  Array[Float]=Array.emptyFloatArray, missing: Float=Float.MaxValue, metadata: Map[String,String]=Map.empty ) extends ArrayBase[Float](shape,origin,missing,metadata) {
  def data: Array[Float] = _data
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data, missing )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data.map(_.toDouble), missing )

  def merge( other: ArrayBase[Float] ): ArrayBase[Float] = HeapFltArray( toCDFloatArray.merge( other.toCDFloatArray ), origin, mergeMetadata("merge",other) )
  def combine( combineOp: CDArray.ReduceOp[Float], other: ArrayBase[Float] ): ArrayBase[Float] = HeapFltArray( CDFloatArray.combine( combineOp, toCDFloatArray, other.toCDFloatArray ), origin, mergeMetadata("merge",other) )
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={missing.toString}> {_data.mkString(",")} </array> % metadata
}
object HeapFltArray {
  def apply( cdarray: CDFloatArray, origin: Array[Int], metadata: Map[String,String] ): HeapFltArray = new HeapFltArray( cdarray.getShape, origin, cdarray.getArrayData(), cdarray.getInvalid, metadata )
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapFltArray = HeapFltArray( CDArray.factory(ucarray,missing), origin, metadata )
}

class HeapDblArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, val _data_ : Array[Double]=Array.emptyDoubleArray, missing: Double=Double.MaxValue, metadata: Map[String,String]=Map.empty ) extends ArrayBase[Double](shape,origin,missing,metadata) {
  def data: Array[Double] = _data_
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data.map(_.toFloat), missing.toFloat )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data, missing )

  def merge( other: ArrayBase[Double] ): ArrayBase[Double] = HeapDblArray( toCDDoubleArray.merge( other.toCDDoubleArray ), origin, mergeMetadata("merge",other) )
  def combine( combineOp: CDArray.ReduceOp[Double], other: ArrayBase[Double] ): ArrayBase[Double] = HeapDblArray( CDDoubleArray.combine( combineOp, toCDDoubleArray, other.toCDDoubleArray ), origin, mergeMetadata("merge",other) )
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={missing.toString} > {_data_.mkString(",")} </array> % metadata
}
object HeapDblArray {
  def apply( cdarray: CDDoubleArray, origin: Array[Int], metadata: Map[String,String] ): HeapDblArray = new HeapDblArray( cdarray.getShape, origin, cdarray.getArrayData(), cdarray.getInvalid, metadata )
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapDblArray = HeapDblArray( CDDoubleArray.factory(ucarray,missing), origin, metadata )
}

class RDDPartition( val iPart: Int, val elements: Map[String,ArrayBase[Float]] , metadata: Map[String,String] ) extends MetadataCarrier(metadata) {
  def ++( other: RDDPartition ): RDDPartition = {
    assert( (iPart==other.iPart) || (iPart == -1) || (other.iPart == -1), "Attempt to merge RDDPartitions with incommensurate partition indices: %d vs %d".format(iPart,other.iPart ) )
    new RDDPartition( if( iPart >= 0 ) iPart else other.iPart, elements ++ other.elements, metadata ++ other.metadata)
  }
  def element( id: String ): Option[ArrayBase[Float]] = elements.get( id )
  def head: ( String, ArrayBase[Float] ) = elements.head
  def toXml: xml.Elem = {
    val values: Iterable[xml.Node] = elements.values.map(_.toXml)
    <partition> {values} </partition>  % metadata
  }
}

object RDDPartition {
  def apply ( iPart: Int = -1, elements: Map[String,ArrayBase[Float]] = Map.empty,  metadata: Map[String,String] = Map.empty ) = new RDDPartition( iPart, elements, metadata )
  def merge( rdd_parts: Seq[RDDPartition] ) = rdd_parts.foldLeft( RDDPartition() )( _ ++ _ )
}

object RDDPartSpec {
  def apply( partition: Partition, varSpecs: List[ RDDVariableSpec ] ): RDDPartSpec = new RDDPartSpec( partition, varSpecs )
}

class RDDPartSpec( val partition: Partition, val varSpecs: List[ RDDVariableSpec ] ) extends Serializable {
  def getRDDPartition: RDDPartition = {
    val elements =  Map( varSpecs.map( vSpec => (vSpec.uid, vSpec.toHeapArray(partition)) ): _* )
 //   println( "\n   RDDPartSpec[%d]: %s".format( partition.index, elements.head._2.data.mkString(",") ) )
    RDDPartition( partition.index, elements )
  }

}

class RDDVariableSpec( val uid: String, val metadata: Map[String,String], val missing: Float, val section: CDSection  ) extends Serializable {
  def toHeapArray( partition: Partition ) = HeapFltArray( partition.dataSection(section, missing), section.getOrigin, metadata )
}

