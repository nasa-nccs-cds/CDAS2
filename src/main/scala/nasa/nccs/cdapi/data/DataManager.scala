package nasa.nccs.cdapi.data

import nasa.nccs.caching.{CachePartition, Partition}
import nasa.nccs.cdapi.cdm.{Collection, NetcdfDatasetMgr, RemapElem, TimeConversionSpec}
import nasa.nccs.cdapi.tensors._
import nasa.nccs.cdas.engine.WorkflowNode
import nasa.nccs.cdas.engine.spark.{RangePartitioner, RecordKey, RecordKey$}
import nasa.nccs.cdas.kernels.{KernelContext, zmqPythonKernel}
import nasa.nccs.cdas.workers.TransVar
import nasa.nccs.esgf.process.{CDSection, TargetGrid}
import nasa.nccs.utilities.{Loggable, cdsutils}
import org.apache.spark.rdd.RDD
import ucar.nc2.constants.AxisType
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.time.CalendarDate

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
          else if( key == "roi")  ( key, CDSection.merge(value,value1) )
          else (key, opName + "(" + value + "," + value1 + ")")
      }
    }
  }
  def mergeMetadata( opName: String, metadata: Iterable[Map[String, String]] ): Map[String, String] = {
    metadata.foldLeft( metadata.head )( mergeMetadata(opName) )
  }
}

abstract class MetadataCarrier( val metadata: Map[String,String] = Map.empty ) extends Serializable with Loggable {
  def mergeMetadata( opName: String, other: MetadataCarrier ): Map[String,String] = MetadataOps.mergeMetadata( opName )( metadata, other.metadata )
  def toXml: xml.Elem
  def attr(id:String): String = metadata.getOrElse(id,"")
  def mdata(): java.util.Map[String,String] = metadata

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

  def getDataRDD( id: String, domain: Map[AxisType,(Int,Int)] ): RDD[RDDRecord]


}

abstract class ArrayBase[T <: AnyVal]( val shape: Array[Int]=Array.emptyIntArray, val origin: Array[Int]=Array.emptyIntArray, val missing: Option[T]=None, metadata: Map[String,String]=Map.empty, val indexMaps: List[CDCoordMap] = List.empty ) extends MetadataCarrier(metadata) with Serializable {

  def data:  Array[T]
  def toCDFloatArray: CDFloatArray
  def toCDDoubleArray: CDDoubleArray
  def toCDLongArray: CDLongArray
  def toUcarFloatArray: ucar.ma2.Array = toCDFloatArray
  def toUcarDoubleArray: ucar.ma2.Array = toCDDoubleArray
  def toCDWeightsArray: Option[CDFloatArray] = None
  def getMetadataStr = metadata map { case ( key, value ) => key + ":" + value } mkString (";")
  def getSampleData( size: Int, start: Int): Array[Float] = toCDFloatArray.getSampleData( size, start )
  def getSampleDataStr( size: Int, start: Int): String = toCDFloatArray.getSampleData( size, start ).mkString( "[ ",", "," ]")
  def uid: String = metadata.getOrElse("uid", metadata.getOrElse("collection","") + ":" + metadata.getOrElse("name",""))
  override def toString = "<array shape=(%s), %s> %s </array>".format( shape.mkString(","), metadata.mkString(","), cdsutils.toString(data.mkString(",")) )

}

class HeapFltArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, private val _data:  Array[Float]=Array.emptyFloatArray, _missing: Option[Float]=None,
                    val gridSpec: String = "", metadata: Map[String,String]=Map.empty, private val _optWeights: Option[Array[Float]]=None, indexMaps: List[CDCoordMap] = List.empty ) extends ArrayBase[Float](shape,origin,_missing,metadata,indexMaps) with Loggable {
  val bb = java.nio.ByteBuffer.allocate(4)
  def data: Array[Float] = _data
  def weights: Option[Array[Float]] = _optWeights
  override def toCDWeightsArray: Option[CDFloatArray] = _optWeights.map( CDFloatArray( shape, _, getMissing() ) )
  def getMissing( default: Float = Float.MaxValue ): Float = _missing.getOrElse(default)
  def sameGrid( other: HeapFltArray) = gridSpec.equals( other.gridSpec )
  def hasData = (_data.length > 0)

  def reinterp( weights: Map[Int,RemapElem], origin_mapper: Array[Int] => Array[Int] ): HeapFltArray = {
    val reinterpArray = toCDFloatArray.reinterp(weights)
    new HeapFltArray( reinterpArray.getShape, origin_mapper(origin), reinterpArray.getArrayData(), Some(reinterpArray.getInvalid), gridSpec, metadata )
  }
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data, getMissing(), indexMaps )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data.map(_.toDouble), getMissing() )
  def toCDLongArray: CDLongArray = CDLongArray( shape, data.map(_.toLong) )
  def verifyGrids( other: HeapFltArray ) = if( !sameGrid(other) ) throw new Exception( s"Error, attempt to combine arrays with different grids: $gridSpec vs ${other.gridSpec}")

  def append( other: HeapFltArray ): HeapFltArray = {
    verifyGrids( other )
    logger.debug( "Appending arrays: {o:(%s), s:(%s)} + {o:(%s), s:(%s)} ".format( origin.mkString(","), shape.mkString(","), other.origin.mkString(","), other.shape.mkString(",")))
    if( origin(0) < other.origin(0) ) {
      if( origin(0) + shape(0) != other.origin(0) ) throw new Exception( "Appending non-contiguous arrays" )
      HeapFltArray(toCDFloatArray.append(other.toCDFloatArray), origin, gridSpec, mergeMetadata("merge", other), toCDWeightsArray.map(_.append(other.toCDWeightsArray.get)))
    } else {
      if( other.origin(0) + other.shape(0) != origin(0) ) throw new Exception( "Appending non-contiguous arrays" )
      HeapFltArray(other.toCDFloatArray.append(toCDFloatArray), other.origin, gridSpec, mergeMetadata("merge", other), other.toCDWeightsArray.map(_.append(toCDWeightsArray.get)))
    }
  }
  def split( index_offset: Int ): ( HeapFltArray, HeapFltArray ) = toCDFloatArray.split(index_offset) match {
      case (a0, a1) =>
        val ( fa0, fa1 ) = ( CDFloatArray(a0), CDFloatArray(a1) )
        val origin1 = origin.zipWithIndex.map{ case (o,i) => if( i == 0 ) ( origin(0) + index_offset ) else o }
        ( new HeapFltArray( fa0.getShape, origin, fa0.getArrayData(), Some(a0.getInvalid), gridSpec, metadata, _optWeights, fa0.getCoordMaps ),
          new HeapFltArray( fa1.getShape, origin1, fa1.getArrayData(), Some(a1.getInvalid), gridSpec, metadata, _optWeights, fa1.getCoordMaps ) )    // TODO: split weights and coord maps?
    }

  def slice( startIndex: Int, size: Int ): HeapFltArray = {
    logger.debug( s"HeapFltArray: slice --> startIndex:{${startIndex}} size:{${size}} ")
    val fa = CDFloatArray(toCDFloatArray.slice(0,startIndex,size))
    val origin1 = origin.zipWithIndex.map{ case (o,i) => if( i == 0 ) ( origin(0) + startIndex ) else o }
    new HeapFltArray( fa.getShape, origin, fa.getArrayData(), Some(fa.getInvalid), gridSpec, metadata, _optWeights, fa.getCoordMaps ) // TODO: split weights and coord maps?
  }

  def toByteArray() = {
    bb.putFloat( 0, missing.getOrElse(Float.NaN) )
    toUcarFloatArray.getDataAsByteBuffer().array() ++ bb.array()
  }
  def combine( combineOp: CDArray.ReduceOp[Float], other: HeapFltArray ): HeapFltArray = {
    verifyGrids( other )
    HeapFltArray( CDFloatArray.combine( combineOp, toCDFloatArray, other.toCDFloatArray ), origin, gridSpec, mergeMetadata("merge",other), toCDWeightsArray.map( _.append( other.toCDWeightsArray.get ) ) )
  }
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={getMissing().toString}> {_data.mkString(",")} </array> % metadata
}
object HeapFltArray {
  def apply( cdarray: CDFloatArray, origin: Array[Int], metadata: Map[String,String], optWeights: Option[Array[Float]] ): HeapFltArray = {
    val gridSpec = metadata.get( "gridfile" ).map( "file:/" + _ ).getOrElse("")
    new HeapFltArray(cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), gridSpec, metadata, optWeights, cdarray.getCoordMaps)
  }
  def apply( shape: Array[Int], origin: Array[Int], metadata: Map[String,String] ): HeapFltArray = {
    val gridSpec = metadata.get( "gridfile" ).map( "file:/" + _ ).getOrElse("")
    new HeapFltArray( shape, origin, Array.emptyFloatArray, None, gridSpec, metadata )
  }
  def apply( cdarray: CDFloatArray, origin: Array[Int], gridSpec: String, metadata: Map[String,String], optWeights: Option[CDFloatArray] ): HeapFltArray = {
    new HeapFltArray(cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), gridSpec, metadata, optWeights.map(_.getArrayData()), cdarray.getCoordMaps)
  }
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], gridSpec: String, metadata: Map[String,String], missing: Float ): HeapFltArray = HeapFltArray( CDArray(ucarray,missing), origin, gridSpec, metadata, None )

  def apply( tvar: TransVar, invalidOpt: Option[Float], _gridSpec: Option[String] = None ): HeapFltArray = {
    val buffer = tvar.getDataBuffer()
    val buff_size = buffer.capacity()
    val ucarray: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, tvar.getShape, buffer )
    val floatArray: CDFloatArray = CDFloatArray.cdArrayConverter(CDArray[Float](ucarray, Float.MaxValue ) )
    val gridSpec = _gridSpec.getOrElse("file:/" + tvar.getMetaData.get("gridfile"))
    new HeapFltArray( tvar.getShape, tvar.getOrigin, floatArray.getStorageArray, invalidOpt, gridSpec, tvar.getMetaData.asScala.toMap )
  }
  def empty(rank:Int) = HeapFltArray( CDFloatArray.empty, Array.fill(rank)(0), "", Map.empty[String,String], None )
  def toHeapFloatArray( fltBaseArray: ArrayBase[Float] ): HeapFltArray = fltBaseArray match {
    case heapFltArray: HeapFltArray => heapFltArray
    case wtf => throw new Exception( "HeapFltArray cast error from ArrayBase[Float]" )
  }
}

class HeapDblArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, val _data_ : Array[Double]=Array.emptyDoubleArray, _missing: Option[Double]=None, metadata: Map[String,String]=Map.empty ) extends ArrayBase[Double](shape,origin,_missing,metadata)  {
  def data: Array[Double] = _data_
  def getMissing( default: Double = Double.MaxValue ): Double = _missing.getOrElse(default)
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data.map(_.toFloat), getMissing().toFloat )
  def toCDLongArray: CDLongArray = CDLongArray( shape, data.map(_.toLong) )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data, getMissing() )

  def append( other: ArrayBase[Double] ): ArrayBase[Double]  = {
    logger.debug( "Appending arrays: {o:(%s), s:(%s)} + {o:(%s), s:(%s)} ".format( origin.mkString(","), shape.mkString(","), other.origin.mkString(","), other.shape.mkString(",")))
    if( origin(0) < other.origin(0) ) {
      assert( origin(0) + shape(0) == other.origin(0), "Appending non-contiguous arrays" )
      HeapDblArray(toCDDoubleArray.append(other.toCDDoubleArray), origin, mergeMetadata("merge", other))
    } else {
      assert( other.origin(0) + other.shape(0) == origin(0), "Appending non-contiguous arrays" )
      HeapDblArray(other.toCDDoubleArray.append(toCDDoubleArray), other.origin, mergeMetadata("merge", other))
    }
  }
  def combine( combineOp: CDArray.ReduceOp[Double], other: ArrayBase[Double] ): ArrayBase[Double] = HeapDblArray( CDDoubleArray.combine( combineOp, toCDDoubleArray, other.toCDDoubleArray ), origin, mergeMetadata("merge",other) )
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={getMissing().toString} > {_data_.mkString(",")} </array> % metadata
}
object HeapDblArray {
  def apply( cdarray: CDDoubleArray, origin: Array[Int], metadata: Map[String,String] ): HeapDblArray = new HeapDblArray( cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), metadata )
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapDblArray = HeapDblArray( CDDoubleArray.factory(ucarray,missing), origin, metadata )
}

class HeapLongArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, val _data_ : Array[Long]=Array.emptyLongArray, _missing: Option[Long]=None, metadata: Map[String,String]=Map.empty ) extends ArrayBase[Long](shape,origin,_missing,metadata)  {
  def data: Array[Long] = _data_
  def getMissing( default: Long = Long.MaxValue ): Long = _missing.getOrElse(default)
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data.map(_.toFloat), Float.MaxValue )
  def toCDLongArray: CDLongArray = CDLongArray( shape, data )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data.map(_.toDouble), Double.MaxValue )

  def append( other: ArrayBase[Long] ): ArrayBase[Long]  = {
    logger.debug( "Appending arrays: {o:(%s), s:(%s)} + {o:(%s), s:(%s)} ".format( origin.mkString(","), shape.mkString(","), other.origin.mkString(","), other.shape.mkString(",")))
    if( origin(0) < other.origin(0) ) {
      assert( origin(0) + shape(0) == other.origin(0), "Appending non-contiguous arrays" )
      HeapLongArray(toCDLongArray.append(other.toCDLongArray), origin, mergeMetadata("merge", other))
    } else {
      assert( other.origin(0) + other.shape(0) == origin(0), "Appending non-contiguous arrays" )
      HeapLongArray(other.toCDLongArray.append(toCDLongArray), other.origin, mergeMetadata("merge", other))
    }
  }
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={getMissing().toString} > {_data_.mkString(",")} </array> % metadata
}
object HeapLongArray {
  def apply( cdarray: CDLongArray, origin: Array[Int], metadata: Map[String,String] ): HeapLongArray = new HeapLongArray( cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), metadata )
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapLongArray = HeapLongArray( CDLongArray.factory(ucarray), origin, metadata )
}

class RDDRecord(val elements: Map[String,HeapFltArray], metadata: Map[String,String] ) extends MetadataCarrier(metadata) {
  def ++( other: RDDRecord ): RDDRecord = {
    new RDDRecord( elements ++ other.elements, metadata ++ other.metadata)
  }
  def hasMultiGrids: Boolean = {
    if( elements.size == 0 ) return false
    val head_shape = elements.head._2.shape
    elements.exists( item => !item._2.shape.sameElements(head_shape) )     // TODO: Compare axes as well?
  }
  def reinterp( conversionMap: Map[Int,TimeConversionSpec] ): RDDRecord = {
    val new_elements = elements.mapValues( array => conversionMap.get(array.shape(0)) match {
      case Some( conversionSpec ) => array.reinterp( conversionSpec.weights, conversionSpec.mapOrigin )
      case None =>
        if( array.shape(0) != conversionMap.values.head.toSize )  throw new Exception( s"Unexpected time conversion input size: ${array.shape(0)} vs ${conversionMap.values.head.toSize}" )
        array
    })
    new RDDRecord( new_elements, metadata )
  }

  def slice( startIndex: Int, size: Int ): RDDRecord = {
    logger.info( s"RDDPartition: slice --> nElems:{${elements.size}} startIndex:{${startIndex}} size:{${size}} ")
    val new_elems = elements.mapValues( _.slice(startIndex,size) )
    new RDDRecord( new_elems, metadata )
  }

  def hasMultiTimeScales( trsOpt: Option[String]=None ): Boolean = {
    if( elements.size == 0 ) return false
    val ntimesteps = elements.values.head.shape(0)
    elements.exists( item => !(item._2.shape(0)==ntimesteps) )
  }
  def append( other: RDDRecord ): RDDRecord = {
    val commonElems = elements.keySet.union( other.elements.keySet )
    val appendedElems: Set[(String,HeapFltArray)] = commonElems flatMap ( key =>
      other.elements.get(key).fold  (elements.get(key) map (e => key -> e))  (e1 => Some( key-> elements.get(key).fold (e1) (e0 => e0.append(e1)))))
    new RDDRecord( Map(appendedElems.toSeq:_*), metadata ++ other.metadata )
  }
//  def split( index: Int ): (RDDPartition,RDDPartition) = { }
  def getShape = elements.head._2.shape
  def getOrigin = elements.head._2.origin
  def element( id: String ): Option[HeapFltArray] = ( elements find { case (key,array) => key.split(':')(0).equals(id) } ) map ( _._2 )
  def findElements( id: String ): Iterable[HeapFltArray] = ( elements filter { case (key,array) => key.split(':').last.equals(id) } ) values
  def empty( id: String ) = { element(id).isEmpty }
  def head: ( String, HeapFltArray ) = elements.head
  def toXml: xml.Elem = {
    val values: Iterable[xml.Node] = elements.values.map(_.toXml)
    <partition> {values} </partition>  % metadata
  }
  def configure( key: String, value: String ): RDDRecord = new RDDRecord( elements, metadata + ( key -> value ) )
}

object RDDRecord {
  def apply ( elements: Map[String,HeapFltArray] = Map.empty,  metadata: Map[String,String] = Map.empty ) = new RDDRecord( elements, metadata )
  def apply ( rdd: RDDRecord ) = new RDDRecord( rdd.elements, rdd.metadata )
  def merge( rdd_parts: Seq[RDDRecord] ) = rdd_parts.foldLeft( RDDRecord() )( _ ++ _ )
  def empty: RDDRecord = { new RDDRecord( Map.empty[String,HeapFltArray], Map.empty[String,String] ) }
}

object RDDPartSpec {
  def apply( partition: CachePartition, tgrid: TargetGrid, varSpecs: List[ RDDVariableSpec ] ): RDDPartSpec = new RDDPartSpec( partition, partition.getPartitionRecordKey(tgrid), varSpecs )
}

class RDDPartSpec(val partition: CachePartition, val timeRange: RecordKey, val varSpecs: List[ RDDVariableSpec ] ) extends Serializable with Loggable {

  def getRDDPartition: RDDRecord = {
    val t0 = System.nanoTime()
    val elements =  Map( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toHeapArray(partition)) ): _* )
    val rv = RDDRecord( elements )
    logger.debug( "RDDPartSpec{ partition = %s }: completed data input in %.4f sec".format( partition.toString, (System.nanoTime() - t0) / 1.0E9) )
    rv
  }

  def getRDDMetaPartition: RDDRecord = {
    val t0 = System.nanoTime()
    val elements =  Map( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toMetaArray(partition)) ): _* )
    val rv = RDDRecord( elements )
    logger.debug( "RDDPartSpec{ partition = %s }: completed data input in %.4f sec".format( partition.toString, (System.nanoTime() - t0) / 1.0E9) )
    rv
  }


  def getPartitionKey( partitioner: RangePartitioner ): RecordKey = partitioner.newPartitionKey( timeRange.center )

  def empty( uid: String ): Boolean = varSpecs.find( _.uid == uid ) match {
    case Some( varSpec ) => varSpec.empty
    case None => true
  }

}

object DirectRDDPartSpec {
  def apply( partition: Partition, tgrid: TargetGrid, varSpecs: List[ DirectRDDVariableSpec ] ): DirectRDDPartSpec = new DirectRDDPartSpec( partition, partition.getPartitionRecordKey(tgrid), varSpecs )
}

class DirectRDDPartSpec(val partition: Partition, val timeRange: RecordKey, val varSpecs: List[ DirectRDDVariableSpec ] ) extends Serializable with Loggable {

  def getRDDRecordSpecs(): IndexedSeq[DirectRDDRecordSpec] = ( 0 until partition.nRecords ) map ( DirectRDDRecordSpec( this, _ ) )

  def empty( uid: String ): Boolean = varSpecs.find( _.uid == uid ) match {
    case Some( varSpec ) => varSpec.empty
    case None => true
  }

}

object DirectRDDRecordSpec {
  def apply( partSpec: DirectRDDPartSpec, iRecord: Int ): DirectRDDRecordSpec = new DirectRDDRecordSpec( partSpec.partition, iRecord, partSpec.timeRange, partSpec.varSpecs )
}

class DirectRDDRecordSpec(val partition: Partition, iRecord: Int, val timeRange: RecordKey, val varSpecs: List[ DirectRDDVariableSpec ] ) extends Serializable with Loggable {

  def getRDDPartition: RDDRecord = {
    val t0 = System.nanoTime()
    val elements =  Map( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toHeapArray(partition,iRecord)) ): _* )
    val rv = RDDRecord( elements )
    logger.debug( "DirectRDDRecordSpec{ partition = %s, record = %d, cube size = %d }: completed data input in %.4f sec".format( partition.toString, iRecord, partition.recordMemorySize, (System.nanoTime() - t0) / 1.0E9) )
    rv
  }

  def empty( uid: String ): Boolean = varSpecs.find( _.uid == uid ) match {
    case Some( varSpec ) => varSpec.empty
    case None => true
  }

}

object ExtRDDPartSpec {
  def apply(timeRange: RecordKey, varSpecs: List[ RDDVariableSpec ] ): ExtRDDPartSpec = new ExtRDDPartSpec( timeRange, varSpecs )
}

class ExtRDDPartSpec(val timeRange: RecordKey, val varSpecs: List[ RDDVariableSpec ] ) extends Serializable with Loggable {

  def getRDDPartition: RDDRecord = {
    val t0 = System.nanoTime()
    val elements =  Map( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toMetaArray ) ): _* )
    val rv = RDDRecord( elements )
    logger.debug( "RDDPartSpec: completed data input in %.4f sec".format( (System.nanoTime() - t0) / 1.0E9) )
    rv
  }

}

class DirectRDDVariableSpec( uid: String, metadata: Map[String,String], missing: Float, section: CDSection, val varShortName: String, val dataPath: String  ) extends RDDVariableSpec( uid, metadata, missing, section  ) with Loggable {
  def toHeapArray( partition: Partition, iRecord: Int ) = {
    val recordSection = partition.recordSection( section.toSection, iRecord )
    val fltData: CDFloatArray =  CDFloatArray.factory( readVariableData( recordSection ), missing )
    logger.debug( "toHeapArray: %s, part[%d]: dim=%d, origin=(%s), shape=[%s]".format( section.toString(), partition.index, partition.dimIndex, recordSection.getOrigin.mkString(","), recordSection.getShape.mkString(",") ) )
    HeapFltArray( fltData, section.getOrigin, metadata, None )
  }
  def readVariableData(section: ma2.Section): ma2.Array =  NetcdfDatasetMgr.readVariableData(varShortName, dataPath, section )
}

class RDDVariableSpec( val uid: String, val metadata: Map[String,String], val missing: Float, val section: CDSection  ) extends Serializable with Loggable {

  def toHeapArray( partition: CachePartition ) = {
    val rv = HeapFltArray( partition.dataSection(section, missing), section.getOrigin, metadata, None )
    logger.debug( "toHeapArray: %s, part[%d]: dim=%d, range=(%d:%d), shape=[%s]".format( section.toString(), partition.index, partition.dimIndex, partition.startIndex, partition.endIndex, partition.shape.mkString(",") ) )
    rv
  }
  def toMetaArray = {
    val rv = HeapFltArray( section.getShape, section.getOrigin, metadata )
    logger.debug( "toHeapArray: %s".format( section.toString() ) )
    rv
  }
  def toMetaArray( partition: Partition ) = {
    val rv = HeapFltArray( partition.partSection( section.toSection( partition.partitionOrigin ) ).getShape, section.getOrigin, metadata )
    logger.debug( "toHeapArray: %s, part[%d]: dim=%d, range=(%d:%d), shape=[%s]".format( section.toString(), partition.index, partition.dimIndex, partition.startIndex, partition.endIndex, partition.shape.mkString(",") ) )
    rv
  }
  def empty = section.getShape.contains(0)
  def rank = section.getShape.length
}

//class RDDRegen(val source: RDD[(PartitionKey,RDDPartition)], val sourceGrid: TargetGrid, val resultGrid: TargetGrid, node: WorkflowNode, kernelContext: KernelContext ) extends Loggable {
//  private val regen: Boolean = !sourceGrid.equals(resultGrid)
//  lazy val regridKernel = getRegridKernel
////  def getRDD(): RDD[(Int,RDDPartition)] = if(regen) {
////    source
////    node.map( source, kernelContext, regridKernel )
////  } else source
//
//  def getRegridKernel(): zmqPythonKernel = node.workflow.executionMgr.getKernel( "python.cdmsmodule", "regrid"  ) match {
//    case pyKernel: zmqPythonKernel => pyKernel
//    case x => throw new Exception( "Unexpected Kernel class for regrid module: " + x.getClass.getName)
//  }
//}


