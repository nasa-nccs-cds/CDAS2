package nasa.nccs.cdapi.data

import org.apache.spark.rdd.RDD
import ucar.nc2.constants.AxisType

// Developer API for integrating various data management and IO frameworks such as SIA-IO and CDAS-Cache.
// It is intended to be deployed on the master node of the analytics server (this is not a client API).

trait RDDataManager {

  def getDatasets(): Set[String]
  def getDatasetMetadata( dsid: String ): Map[String,String]

  def getVariables( dsid: String ): Set[String]
  def getVariableMetadata( vid: String ): Map[String,String]

  def getDataProducts(): Set[String] = List.empty
  def getDataProductMetadata( pid: String ): Map[String,String] = Map.empty

  def getDataRDD( id: String, domain: Map[AxisType,(Int,Int)] ): RDD[RDDPartition]

}

abstract class ArrayBase( val shape: Array[Int], val missing: Float ) {
  def data:  Array[Float]
}

class HeapArray( val shape: Array[Int], val _data:  Array[Float], val missing: Float ) {
  def data: Array[Float] = _data
}

class RDDPartition( val elements: Map[String,ArrayBase], val metadata: Map[String,String] ) {


}
