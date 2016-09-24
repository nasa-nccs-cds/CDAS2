package nasa.nccs.cdapi.data

import nasa.nccs.cdapi.tensors.CDFloatArray
import org.apache.spark.rdd.RDD
import ucar.nc2.constants.AxisType

// Developer API for integrating various data management and IO frameworks such as SIA-IO and CDAS-Cache.
// It is intended to be deployed on the master node of the analytics server (this is not a client API).

trait RDDataManager {

  def getDatasets(): Set[String]
  def getDatasetMetadata( dsid: String ): Map[String,String]

  def getVariables( dsid: String ): Set[String]
  def getVariableMetadata( vid: String ): Map[String,String]

  def getDataProducts(): Set[String] = Set.empty
  def getDataProductMetadata( pid: String ): Map[String,String] = Map.empty

  def getDataRDD( id: String, domain: Map[AxisType,(Int,Int)] ): RDD[RDDPartition]

}

abstract class ArrayBase( val shape: Array[Int], val missing: Float, val metadata: Map[String,String] ) {
  def data:  Array[Float]
  def toCDFloatArray: CDFloatArray
  def mergeMetadata( opName: String, other: ArrayBase ): Map[String,String]
}

class HeapArray( shape: Array[Int], private val _data:  Array[Float], missing: Float, metadata: Map[String,String] ) extends ArrayBase(shape,missing,metadata) {
  def data: Array[Float] = _data
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data, missing )

  def mergeMetadata( opName: String, other: ArrayBase ): Map[String, String] = metadata map { case (key, value) =>
    other.metadata.get(key) match {
      case None => (key, value)
      case Some(value1) =>
        if (value == value1) (key, value)
        else (key, opName + "(" + value + "," + value1 + ")" )
    }
  }
}

object HeapArray {
  def apply( cdarray: CDFloatArray, metadata: Map[String,String] ): HeapArray = new HeapArray( cdarray.getShape, cdarray.getArrayData(), cdarray.getInvalid, metadata )
}

class RDDPartition( val iPart: Int, val elements: Map[String,ArrayBase] , val metadata: Map[String,String] ) {
  def ++( other: RDDPartition ): RDDPartition = {
    assert( (iPart==other.iPart) || (iPart == -1) || (other.iPart == -1), "Attempt to merge RDDPartitions with incommensurate partition indices: %d vs %d".format(iPart,other.iPart ) )
    new RDDPartition( if( iPart >= 0 ) iPart else other.iPart, elements ++ other.elements, metadata ++ other.metadata)
  }
  def getElement( id: String ): Option[ArrayBase] = elements.get( id )
}

object RDDPartition {
  def apply ( iPart: Int = -1, elements: Map[String,ArrayBase] = Map.empty,  metadata: Map[String,String] = Map.empty ) = new RDDPartition( iPart, elements, metadata )
  def merge( rdd_parts: Seq[RDDPartition] ) = rdd_parts.foldLeft( RDDPartition() )( _ ++ _ )
}
