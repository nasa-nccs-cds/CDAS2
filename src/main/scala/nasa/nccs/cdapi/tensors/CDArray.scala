// Based on ucar.ma2.Array, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

package nasa.nccs.cdapi.tensors
import java.nio._

import nasa.nccs.cdapi.cdm.RemapElem
import nasa.nccs.cdapi.data.FastMaskedArray
import nasa.nccs.cdapi.tensors.CDArray.{FlatIndex, StorageIndex}
import nasa.nccs.utilities.{Loggable, cdsutils}
import ucar.ma2
import ucar.ma2.{ArrayFloat, Index, IndexIterator}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object CDArray {

  type ReduceOp[T] = (T,T)=>T
  type ReduceWNOp[T] = (Iterable[T],T)=>(T,Float)
  type ReduceNOp[T] = (Iterable[T],T)=>T
  type StorageIndex = Int
  type FlatIndex = Int

  def apply[T <: AnyVal]( shape: Array[Int], storage: Buffer, invalid: T, indexMap: Map[Int,CDCoordMap] = Map.empty ): CDArray[T] = apply[T]( CDIndexMap(shape,indexMap), storage, invalid )

  def apply[T <: AnyVal]( index: CDIndexMap, storage: Buffer, invalid: T ): CDArray[T] = {
    storage match {
      case x: FloatBuffer => new CDFloatArray( index, storage.asInstanceOf[FloatBuffer], invalid.asInstanceOf[Float] ).asInstanceOf[CDArray[T]]
      case x: IntBuffer => new CDIntArray( index, storage.asInstanceOf[IntBuffer] ).asInstanceOf[CDArray[T]]
      case x: ByteBuffer => new CDByteArray( index, storage.asInstanceOf[ByteBuffer] ).asInstanceOf[CDArray[T]]
      case x: ShortBuffer => new CDShortArray( index, storage.asInstanceOf[ShortBuffer] ).asInstanceOf[CDArray[T]]
      case x: DoubleBuffer => new CDDoubleArray( index, storage.asInstanceOf[DoubleBuffer], invalid.asInstanceOf[Double] ).asInstanceOf[CDArray[T]]
      case x: LongBuffer => new CDLongArray( index, storage.asInstanceOf[LongBuffer] ).asInstanceOf[CDArray[T]]
      case x => throw new Exception( "Unsupported elem type in CDArray: " + x)
    }
  }

  def toBuffer[T]( array: Array[T] ): Buffer = array.head match {
    case u: Float  => FloatBuffer.wrap( array.asInstanceOf[Array[Float]] )
    case u: Double  => DoubleBuffer.wrap( array.asInstanceOf[Array[Double]] )
    case u: Int  => IntBuffer.wrap( array.asInstanceOf[Array[Int]] )
    case u: Short  => ShortBuffer.wrap( array.asInstanceOf[Array[Short]] )
    case u: Long  =>  LongBuffer.wrap( array.asInstanceOf[Array[Long]] )
    case x                => ByteBuffer.wrap( array.asInstanceOf[Array[Byte]] )
  }

  def toBuffer( array: ucar.ma2.Array ): Buffer = array.getElementType.toString match {
    case "float"  => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]] )
    case "double" => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]] )
    case "int"    => IntBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]] )
    case "short"  => ShortBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]] )
    case "long"  =>  LongBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Long]] )
    case x                => ByteBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Byte]] )
  }

  def apply[T <: AnyVal]( array: ucar.ma2.Array, invalid: T ): CDArray[T] = apply( new CDIndexMap( array.getShape ), toBuffer( array), invalid )
  def apply[T <: AnyVal]( shape: Array[Int], array: Array[T], invalid: T ): CDArray[T] = apply( new CDIndexMap( shape ), toBuffer( array), invalid )

  def getDataType( storage: Buffer ): ma2.DataType = storage match {
    case x: DoubleBuffer => ma2.DataType.DOUBLE
    case x: FloatBuffer => ma2.DataType.FLOAT
    case x: IntBuffer => ma2.DataType.INT
    case x: ShortBuffer => ma2.DataType.SHORT
    case x: LongBuffer => ma2.DataType.LONG
    case x: ByteBuffer => ma2.DataType.BYTE
  }


}

abstract class CDArray[ T <: AnyVal ]( private val cdIndexMap: CDIndexMap, private val storage: Buffer ) extends Loggable with Serializable  {
  protected val rank = cdIndexMap.getRank
  protected val dataType = CDArray.getDataType( storage )

  def isStorageCongruent: Boolean = cdIndexMap.isStorageCongruent(getStorageSize)
  def getInvalid: T
  def getStorageSize: Int = storage.capacity
  def getElementSize: Int = dataType.getSize
  def getIndex: CDIndexMap = new CDIndexMap( this.cdIndexMap )
  def dup() = CDArray[T]( cdIndexMap, storage, getInvalid )
  def getIterator: CDIterator =  if(isStorageCongruent) { new CDStorageIndexIterator( cdIndexMap ) } else { CDIterator.factory( cdIndexMap ) }
  def getRank: Int = rank
  def getShape: Array[Int] = cdIndexMap.getShape
  def getStride: Array[Int] = cdIndexMap.getStride
  def getOffset: Int = cdIndexMap.getOffset
  def getStorageShape: Array[Int] = cdIndexMap.getStorageShape
  def getSubspaceSize(subAxes: Array[Int]): Int = subAxes.map(getShape(_)).foldLeft(1)(_*_)
  def getStorageValue( index: StorageIndex ): T
  def getValue( indices: Array[Int] ): T = getStorageValue( cdIndexMap.getStorageIndex(indices) )
  def getFlatValue( index: FlatIndex ): T = getStorageValue( getIterator.flatToStorage(index) )
  def setStorageValue( index: StorageIndex, value: T ): Unit
  def setValue( indices: Array[Int], value: T ): Unit = { setStorageValue( cdIndexMap.getStorageIndex(indices), value )   }
  def setFlatValue( index: FlatIndex, value: T  ): Unit = setStorageValue( getIterator.flatToStorage(index), value )
  def getSize: Int =  cdIndexMap.getSize
  def getSizeBytes: Int =  cdIndexMap.getSize * getElementSize
  def getRangeIterator(ranges: List[ma2.Range] ): CDIterator = section(ranges).getIterator
  def getStorage: Buffer = { storage }
  def copySectionData( maxCopySize: Int = Int.MaxValue ): Buffer
  def getSectionData(maxCopySize: Int = Int.MaxValue): Buffer = if( isStorageCongruent ) getStorage else copySectionData(maxCopySize)
  def section( subsection: ma2.Section ): CDArray[T] = section( subsection.getRanges.toList )
  def section(ranges: List[ma2.Range]): CDArray[T] = createView(cdIndexMap.section(ranges))
  def valid( value: T ): Boolean
  def spawn( shape: Array[Int], fillval: T ): CDArray[T]
  def spawn( index: CDIndexMap, fillval: T ): CDArray[T]
  def sameShape( cdIndex: CDIndexMap ): Boolean = cdIndexMap.getShape.sameElements( cdIndex.getShape )
  def sameStorage( cdIndex: CDIndexMap ): Boolean = ( cdIndexMap.getStride.sameElements( cdIndex.getStride ) && ( cdIndexMap.getOffset == cdIndex.getOffset ) )
  def sameShape[R <: AnyVal]( array: CDArray[R] ): Boolean = sameShape( array.getIndex )
  def sameStorage[R <: AnyVal]( array: CDArray[R] ): Boolean = sameStorage( array.getIndex )
  def getAccumulationIndex( reduceDims: Array[Int], coordMaps: List[CDCoordMap] = List.empty ): CDIndexMap = this.cdIndexMap.getAccumulator( reduceDims, coordMaps )
  def getSectionArray( maxSize: Int = Int.MaxValue ): Array[T]
  def getAccumulator( reduceDims: Array[Int], fillval: T, coordMapOpt: Option[CDCoordMap] = None ): CDArray[T] =
    spawn( cdIndexMap.getAccumulator( reduceDims, coordMapOpt ), fillval )

  def split( index_offset: Int ): ( CDArray[T], CDArray[T] ) = {
    val shape = cdIndexMap.getShape
    val shape0 = shape.zipWithIndex.map{ case (s,i) => if( i == 0 ) index_offset else s }
    val origin0 = Array.fill[Int](shape0.length)(0)
    val shape1 = shape.zipWithIndex.map{ case (s,i) => if( i == 0 ) ( shape(0) - index_offset ) else s }
    val origin1 = origin0.zipWithIndex.map{ case (o,i) => if( i == 0 ) index_offset else o }
    ( section( new ma2.Section(origin0,shape0) ), section( new ma2.Section(origin1,shape1) ) )
  }

  def getReducedArray: CDArray[T] = CDArray[T]( getStorageShape, storage, getInvalid, cdIndexMap.getCoordMap )

//  def reduce( reductionOp: CDArray.ReduceOp[T], reduceDims: Array[Int], initVal: T, coordMapOpt: Option[CDCoordMap] = None ): CDArray[T] = {
//    val fullShape = coordMapOpt match { case Some(coordMap) => coordMap.mapShape( getShape ); case None => getShape }
//    val accumulator: CDArray[T] = getAccumulatorArray( reduceDims, initVal, fullShape )
//    val iter = getIterator
//    coordMapOpt match {
//      case Some(coordMap) => {
//        for (index <- iter; array_value = getStorageValue(index); if valid(array_value); coordIndices = iter.getCoordinateIndices) {
//          val mappedCoords = coordMap.map(coordIndices)
//          accumulator.setValue(mappedCoords, reductionOp(accumulator.getValue(mappedCoords), array_value))
//        }
//      }
//      case None => {
//        for (index <- iter; array_value = getStorageValue(index); coordIndices = iter.getCoordinateIndices) {
//          if (valid(array_value)) {
//            val reduced_value = reductionOp(accumulator.getValue(coordIndices), array_value)
//            accumulator.setValue(coordIndices, reduced_value)
//          }
//        }
//      }
//    }
//    val rv = accumulator.getReducedArray
//    rv
//  }

  def reduce( reductionOp: CDArray.ReduceOp[T], reduceDims: Array[Int], initVal: T )(implicit tag: ClassTag[T]): CDArray[T] = {
    if( reduceDims.isEmpty ) {
      var value: T = initVal
      val t0 = System.nanoTime()
      var isInvalid = true
      for ( index <-( 0 until getSize ) ) {
        val dval = getStorageValue( index )
        if( valid(dval) ) {
          value = reductionOp(value, dval)
          isInvalid = false
        }
      }
      if( isInvalid ) value = getInvalid
      logger.info( s"Computing reduce, time = %.4f sec".format( (System.nanoTime() - t0) / 1.0E9) )
      val shape = Array.fill[Int](getRank)(1)
      CDArray[T]( shape, Array[T](value), getInvalid )
    } else {
      val t0 = System.nanoTime()
      val accumulator: CDArray[T] = getAccumulator(reduceDims, initVal)
      val iter = getIterator
      for (index <- iter; array_value = getStorageValue(index); coordIndices = iter.getCoordinateIndices) {
        if (valid(array_value)) {
          val v0 = accumulator.getValue(coordIndices)
          val reduced_value = reductionOp( v0, array_value)
          accumulator.setValue(coordIndices, reduced_value)
 //         logger.info( "I-%d %.2f %.2f %.2f: coords = (%s), val = %.2f".format( accumulator.getIndex.getStorageIndex(coordIndices), array_value, v0, reduced_value, coordIndices.mkString(", "), accumulator.getValue(coordIndices) ) )
        }
      }
      val rv = accumulator.getReducedArray
      logger.info( s"Computing generalized reduce, time = %.4f sec, value = %.2f".format( (System.nanoTime() - t0) / 1.0E9, rv.getStorageValue(0) ) )
      rv
    }
  }


  def createRanges( origin: Array[Int], shape: Array[Int], strideOpt: Option[Array[Int]] = None ): List[ma2.Range] = {
    val strides: Array[Int] = strideOpt match {
      case Some(stride_array) => stride_array
      case None => Array.fill[Int](origin.length)(1)
    }
    val rangeSeq = for (i <- origin.indices ) yield {
      if (shape(i) < 0) ma2.Range.VLEN
      else new ma2.Range(origin(i), origin(i) + strides(i) * shape(i) - 1, strides(i))
    }
    rangeSeq.toList
  }

  def section(origin: Array[Int], shape: Array[Int], strideOpt: Option[Array[Int]]=None): CDArray[T] = {
    logger.debug( s"Array: {${getShape.mkString(",")}} --> Section: ORIGIN:{${origin.mkString(",")}} SHAPE:{${shape.mkString(",")}} ")
    createView(cdIndexMap.section(createRanges(origin, shape, strideOpt)))
  }

  def slice(dim: Int, value: Int, size: Int=1): CDArray[T] = {
    logger.debug( s"CDArray: slice --> dim {${dim}}  value:{${value}} size:{${size}} ")
    val origin: Array[Int] = new Array[Int](rank)
    val shape: Array[Int] = getShape
    origin(dim) = value
    shape(dim) = size
    section(origin, shape)
  }

//  def broadcast(dim: Int, size: Int ): CDArray[T] = createView( cdIndexMap.broadcast(dim,size) )
  def broadcast(shape: Array[Int] ): CDArray[T] = createView( cdIndexMap.broadcast(shape) )
//  def flip(dim: Int): CDArray[T] = createView(cdIndexMap.flip(dim))
//  def transpose(dim1: Int, dim2: Int): CDArray[T] = createView(cdIndexMap.transpose(dim1, dim2))
//  def permute(dims: Array[Int]): CDArray[T] = createView(cdIndexMap.permute(dims))

  def reshape(shape: Array[Int]): CDArray[T] = {
    if( shape.product != getSize ) throw new IllegalArgumentException("reshape arrays must have same total size")
    CDArray( new CDIndexMap(shape), getSectionData(), getInvalid )
  }

  def reduce: CDArray[T] = {
    val ri: CDIndexMap = cdIndexMap.reduce
    if (ri eq cdIndexMap) return this
    createView(ri)
  }

  def reduce(dim: Int): CDArray[T] = createView(cdIndexMap.reduce(dim))
  def isVlen: Boolean = false

  protected def createView( cdIndexMap: CDIndexMap ): CDArray[T] = CDArray(cdIndexMap, storage, getInvalid )


  override def toString: String = "Index: " + this.cdIndexMap.toString

  def toDataString: String = "Index: " + this.cdIndexMap.toString + "\n Data = " + mkDataString("[ ",", "," ]")
  def mkDataString( sep: String ): String = getSectionArray().map( _.toString ).mkString( sep )
  def mkDataString( start: String, sep: String, end: String ): String = getSectionArray().map( _.toString ).mkString( start, sep, end )
  def mkBoundedDataString( sep: String, maxSize: Int ): String =  getSectionArray(maxSize).map(_.toString).mkString(sep)
  def mkBoundedDataString( start: String, sep: String, end: String, maxSize: Int ): String = getSectionArray(maxSize).map( _.toString ).mkString( start, sep, end )

}

class CustomException( msg: String ) extends Exception(msg)

object CDFloatArray extends Loggable with Serializable {
  type ReduceOpFlt = CDArray.ReduceOp[Float]
  type ReduceWNOpFlt = CDArray.ReduceWNOp[Float]
  type ReduceNOpFlt = CDArray.ReduceNOp[Float]
  implicit def cdArrayConverter( target: CDArray[Float] ): CDFloatArray = new CDFloatArray( target.getIndex, target.getStorage.asInstanceOf[ FloatBuffer ], target.getInvalid )
  implicit def toUcarArray( target: CDFloatArray ): ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, target.getShape, target.getSectionData().array() )
  val bTrue: Byte = 1
  val bFalse: Byte = 0
  def CountCombine( op: ReduceOpFlt, invalid: Float )( elem: (Float,Float), value: Float ): (Float,Float) = if( value == invalid ) { elem } else ( op( elem._1, value ), elem._2 + 1f )
  def errorOp(opName: String): ReduceOpFlt = (x:Float, y:Float) => { throw new Exception( "Unrecognized Op: " + opName ) }
  val addOp: ReduceOpFlt = (x:Float, y:Float) => ( x + y )
  val sqAddOp: ReduceOpFlt = (x:Float, y:Float) => ( x + y*y )
  val addOpN: ReduceWNOpFlt = ( vals: Iterable[Float], invalid: Float ) => vals.foldLeft[(Float,Float)]((0f,0f))( CountCombine(addOp,invalid) )
  val aveOpN: ReduceNOpFlt = ( vals: Iterable[Float], invalid: Float ) => { val (sum,count) = vals.foldLeft[(Float,Float)]((0f,0f))( CountCombine(addOp,invalid) ); if(count == 0) invalid else sum/count }
  val subtractOp: ReduceOpFlt = (x:Float, y:Float) => ( x - y )
  val multiplyOp: ReduceOpFlt = (x:Float, y:Float) => ( x * y )
  val divideOp: ReduceOpFlt = (x:Float, y:Float) => ( x / y )
  val maxOp: ReduceOpFlt = (x:Float, y:Float) => ( if( x > y ) x else y )
  val customOp: ReduceOpFlt = (x:Float, y:Float) => throw new CustomException( "Failover to custom OP")
  val minOp: ReduceOpFlt = (x:Float, y:Float) => ( if( x < y ) x else y )
  val maxMagOp: ReduceOpFlt = (x:Float, y:Float) => { val xm = Math.abs(x); val ym = Math.abs(y); if( xm > ym ) xm else ym }
  val minMagOp: ReduceOpFlt = (x:Float, y:Float) => { val xm = Math.abs(x); val ym = Math.abs(y); if( xm < ym ) xm else ym }
  val eqOp: ReduceOpFlt = (x:Float, y:Float) => ( y )
  def getOp( opName: String ): ReduceOpFlt = opName match {
    case x if x.startsWith("sum") => addOp
    case x if x.startsWith("ave") => addOp
    case x if x.startsWith("add") => addOp
    case x if x.startsWith("sqAdd") => sqAddOp
    case x if x.startsWith("sub") => subtractOp
    case x if x.startsWith("mul") => multiplyOp
    case x if x.startsWith("div") => divideOp
    case x if x.startsWith("max") => maxOp
    case "min" => minOp
    case "custom" => customOp
    case _ => errorOp( opName )
  }

  def apply( cdIndexMap: CDIndexMap, floatData: Array[Float], invalid: Float ): CDFloatArray  = new CDFloatArray( cdIndexMap, FloatBuffer.wrap(floatData),  invalid )
  def apply( shape: Array[Int], floatData: Array[Float], invalid: Float, indexMaps: List[CDCoordMap] = List.empty ): CDFloatArray  = new CDFloatArray( CDIndexMap(shape,indexMaps), FloatBuffer.wrap(floatData),  invalid )
  def apply( floatData: Array[Float], invalid: Float ): CDFloatArray  = new CDFloatArray( CDIndexMap(Array(floatData.length),List.empty), FloatBuffer.wrap(floatData),  invalid )
  def apply( target: CDArray[Float] ): CDFloatArray  = CDFloatArray.cdArrayConverter( target )
  def const( shape: Array[Int], value: Float ): CDFloatArray = apply( CDIndexMap.const(shape), Array(value), Float.MaxValue )

  def toFloatBuffer( array: ucar.ma2.Array ): FloatBuffer = array.getElementType.toString match {
    case "float"  => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]] )
    case "double" => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]].map( _.toFloat ) )
    case "int"    => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]].map( _.toFloat ) )
    case "short"  => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]].map( _.toFloat ) )
    case "long"  =>  FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Long]].map( _.toFloat ) )
    case x        => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Byte]].map( _.toFloat ) )
  }

  def toFloatArray( array: ucar.ma2.Array ): Array[Float] = array.getElementType.toString match {
    case "float"  => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]]
    case "double" => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]].map( _.toFloat )
    case "int"    => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]].map( _.toFloat )
    case "long"    => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Long]].map( _.toFloat )
    case "short"  => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]].map( _.toFloat )
  }

  def empty: CDFloatArray = { new CDFloatArray( CDIndexMap.empty, FloatBuffer.allocate(0), Float.MaxValue ) }

  def toFloatBuffer( buffer: Buffer ): FloatBuffer = buffer match {
    case x: FloatBuffer  => buffer.asInstanceOf[ FloatBuffer ]
    case x => throw new Exception( "Attempt to convert non-float buffer to FloatBuffer")
  }

  def factory( array: ucar.ma2.Array, invalid: Float, maskOpt: Option[CDByteArray]= None ): CDFloatArray = {
    val storage = CDFloatArray.toFloatBuffer( array )
    val unmasked_array = new CDFloatArray(new CDIndexMap(array.getShape), storage, invalid )
    maskOpt match { case None => unmasked_array; case Some(mask) => applyMask( unmasked_array, mask ) }
  }

  def toArray(buffer: FloatBuffer): Array[Float] = if( buffer.hasArray ) buffer.array else { val data =for( index: Int <- (0 until buffer.capacity) ) yield buffer.get( index ); data.toArray }

  def applyMask( data: CDFloatArray, mask: CDByteArray ): CDFloatArray = {
    assert( data.getSize == mask.getSize, "Error, mask size does not match data size: %d vs %d".format(data.getSize, mask.getSize) )
    val full_mask = mask.broadcast( data.getShape )
    val iter = data.getIterator
    val masked_data = for (index <- iter; dval = data.getStorageValue(index); if data.valid(dval); coordIndices = iter.getCoordinateIndices; maskval = full_mask.getValue(coordIndices))
      yield if( maskval == bTrue ) dval else data.invalid
    new CDFloatArray( data.getIndex, FloatBuffer.wrap( masked_data.toArray ), data.invalid )
  }

  def spawn( shape: Array[Int], f: (Array[Int]) => Float, invalid: Float ): CDFloatArray = {
    val new_array: CDFloatArray = CDArray( shape, FloatBuffer.wrap( Array.fill[Float]( shape.product )(0f) ), invalid  )
    val cdIndexMap = new CDIndexMap( shape )
    val iter = CDIterator.factory( cdIndexMap )
    for ( index <- iter; coords = iter.getCoordinateIndices; value = f(coords) ) new_array.setValue( coords, value )
    new_array
  }

  def combine( reductionOp: ReduceOpFlt, input0: CDFloatArray, input1: CDFloatArray ): CDFloatArray = {
    val sameStructure = input0.getStride.sameElements(input1.getStride)
    val iter = MultiArrayIterator(input0,input1)
    val result = for (flatIndex <- iter; values = iter.values; v0 = values.head; v1 = values.last ) yield
        if ( (v0 == iter.invalid) || (v1 == iter.invalid) || v0.isNaN || v1.isNaN ) iter.invalid else reductionOp( v0, v1 )
    new CDFloatArray( iter.getShape, FloatBuffer.wrap(result.toArray), iter.invalid )
  }

  def combine( reductionNOp: ReduceWNOpFlt, inputs: Iterable[CDFloatArray] ): ( CDFloatArray, CDFloatArray) = {
    val iter = MultiArrayIterator(inputs)
    val dataSize = inputs.head.getIndex.getSize
    val valueBuffer = FloatBuffer.allocate( dataSize )
    val countBuffer = FloatBuffer.allocate( dataSize )
    val result = for (flatIndex <- iter; values = iter.values ) yield {
      val (value, count) = reductionNOp( values, iter.invalid )
      valueBuffer.put( flatIndex, if( count == 0 ) iter.invalid else value )
      countBuffer.put( flatIndex, count )
    }
    ( new CDFloatArray( iter.getShape, valueBuffer, iter.invalid ),  new CDFloatArray( iter.getShape, countBuffer, iter.invalid ) )
  }

  def combine( reductionNOp: ReduceNOpFlt, inputs: Iterable[CDFloatArray] ): CDFloatArray = {
    val iter = MultiArrayIterator(inputs)
    val dataSize = inputs.head.getIndex.getSize
    val results: Iterator[Float] = for (flatIndex <- iter; values = iter.values ) yield reductionNOp( values, iter.invalid )
    new CDFloatArray( iter.getShape, FloatBuffer.wrap(results.toArray), iter.invalid )
  }

  def combine( reductionOp: ReduceOpFlt, input: CDFloatArray, mappedInput: CDFloatArray, coordMap: CDCoordMap ): CDFloatArray = {
    logger.info( "CDFloatArray.combine: input shape=%s, mappedInput shape=%s".format( input.getShape.mkString(","), mappedInput.getShape.mkString(",") ) )
    val iter = new CDArrayIndexIterator( input.getIndex  )
    val result = for (flatIndex <- iter; coords = iter.getCoordinateIndices; mappedCoords = coordMap.map(coords); v0 = input.getValue(coords); v1 = mappedInput.getValue(mappedCoords) ) yield {
      if (!input.valid(v0)) input.invalid else if (!mappedInput.valid(v1)) mappedInput.invalid else reductionOp(v0, v1)
    }
    new CDFloatArray( iter.getShape, FloatBuffer.wrap(result.toArray), input.invalid )
  }

  def accumulate( reductionOp: ReduceOpFlt, input0: CDFloatArray, input1: CDFloatArray ): Unit = {
    val sameStructure = input0.getStride.sameElements(input1.getStride)
    val iter = MultiArrayIterator(input0,input1)
    for ( flatIndex <- iter;  values = iter.values; v0 = values.head; v1 = values.last; if (v0 != iter.invalid) && (v1 != iter.invalid) && !v1.isNaN && !v0.isNaN ) {
      input0.setStorageValue(flatIndex, reductionOp(v0, v1))
    }
  }

  def combine( reductionOp: ReduceOpFlt, input0: CDFloatArray, fval: Float ): CDFloatArray = {
    val result = for( flatIndex <- input0.getIterator; v0 = input0.getStorageValue(flatIndex) ) yield
      if( !input0.valid( v0 ) ) input0.invalid
      else reductionOp(v0,fval)
    new CDFloatArray( input0.getShape, FloatBuffer.wrap(result.toArray), input0.invalid )
  }
}

class CDFloatArray( cdIndexMap: CDIndexMap, val floatStorage: FloatBuffer, protected val invalid: Float ) extends CDArray[Float](cdIndexMap,floatStorage) {
  import CDFloatArray._
  def getStorageValue( index: StorageIndex ): Float = {
    try{ floatStorage.get( index ) } catch {
      case ex: Exception =>
        Float.NaN
    }
  }

  def sample(size: Int): CDFloatArray = CDFloatArray(getSectionArray(size),getInvalid)

  def reinterp( weights: Map[Int,RemapElem] ): CDFloatArray = {
    val slices: Iterable[CDFloatArray] = for ( (i,elem) <- weights ) yield { slice(0,elem.index,1) * elem.weight0 + slice(0,elem.index+1,1) * elem.weight1 }
    slices.reduce( _ append _ )
  }
  def getSampleData( size: Int, start: Int): Array[Float] = {
    val data = floatStorage.array()
    val end = Math.min( start+size, data.length )
    if( start >= end ) { Array.emptyFloatArray }
    else { ( ( start until end ) map { index => data(index) } ).toArray }
  }
  def setStorageValue( index: StorageIndex, value: Float ): Unit = floatStorage.put( index, value )
  def this( shape: Array[Int], storage: FloatBuffer, invalid: Float ) = this( CDIndexMap(shape, List.empty), storage, invalid )
  def this( storage: FloatBuffer, invalid: Float ) = this( CDIndexMap( Array( storage.capacity()), List.empty ), storage, invalid )
  protected def getData: FloatBuffer = floatStorage
  override def getSectionData(maxCopySize: Int = Int.MaxValue): FloatBuffer = {
    if( getSize > 0 ) { super.getSectionData(maxCopySize).asInstanceOf[FloatBuffer] } else FloatBuffer.allocate(0)
  }
  def getStorageData: FloatBuffer = floatStorage
  def isMapped: Boolean = !floatStorage.hasArray
  def getCoordMaps = cdIndexMap.getCoordMaps
  def getStorageArray: Array[Float] = CDFloatArray.toArray( floatStorage )
  def getSectionArray( maxSize: Int = Int.MaxValue ): Array[Float] = {
    val data = CDFloatArray.toArray( getSectionData(maxSize) )
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }
  def getArrayData( maxSize: Int = Int.MaxValue ): Array[Float]  = {
    val data = if( isStorageCongruent ) getStorageArray else getSectionArray()
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }
  override def dup(): CDFloatArray = new CDFloatArray( cdIndexMap.getShape, this.getSectionData(), invalid )
  def valid( value: Float ) = { !value.isNaN && (value != invalid) }
  def toCDFloatArray( target: CDArray[Float] ) = new CDFloatArray( target.getIndex, target.getStorage.asInstanceOf[FloatBuffer], invalid )
  def spawn( shape: Array[Int], fillval: Float ): CDArray[Float] = CDArray( shape, FloatBuffer.wrap(Array.fill[Float]( shape.product )(fillval)), invalid  )
  def spawn( index: CDIndexMap, fillval: Float ): CDArray[Float] = {
    val newshape = index.getStorageShape
    CDArray( index, FloatBuffer.wrap(Array.fill[Float]( newshape.product )(fillval)), invalid  )
  }
  def zeros: CDFloatArray = new CDFloatArray( getShape, FloatBuffer.wrap( Array.fill[Float]( getSize )(0) ), invalid )
  def invalids: CDFloatArray = new CDFloatArray( getShape, FloatBuffer.wrap( Array.fill[Float]( getSize )(invalid) ), invalid )
  def getInvalid = invalid
  def toHeap = new CDFloatArray( cdIndexMap, if(isMapped) copySectionData() else getData, getInvalid )

  def -(array: CDFloatArray) = CDFloatArray.combine( subtractOp, this, array )
  def -=(array: CDFloatArray) = CDFloatArray.accumulate( subtractOp, this, array )
  def +(array: CDFloatArray) = CDFloatArray.combine( addOp, this, array )
  def +=(array: CDFloatArray) = CDFloatArray.accumulate( addOp, this, array )
  def /(array: CDFloatArray) = CDFloatArray.combine( divideOp, this, array )
  def *(array: CDFloatArray) = CDFloatArray.combine( multiplyOp, this, array )
  def /=(array: CDFloatArray) = CDFloatArray.accumulate( divideOp, this, array )
  def *=(array: CDFloatArray) = CDFloatArray.accumulate( multiplyOp, this, array )
  def -(value: Float) = CDFloatArray.combine( subtractOp, this, value )
  def +(value: Float) = CDFloatArray.combine( addOp, this, value )
  def /(value: Float) = CDFloatArray.combine( divideOp, this, value )
  def *(value: Float) = CDFloatArray.combine( multiplyOp, this, value )
  def :=(value: Float) = CDFloatArray.combine( eqOp, this, value )

  def max(reduceDims: Array[Int]=Array.emptyIntArray): CDFloatArray = reduce( maxOp, reduceDims, -Float.MaxValue )
  def min(reduceDims: Array[Int]=Array.emptyIntArray): CDFloatArray = reduce( minOp, reduceDims, Float.MaxValue )
  def maxMag(reduceDims: Array[Int]=Array.emptyIntArray): CDFloatArray =
    reduce( maxMagOp, reduceDims, 0f )
  def minMag(reduceDims: Array[Int]=Array.emptyIntArray): CDFloatArray = reduce( minMagOp, reduceDims, Float.MaxValue )
  def sum(reduceDims: Array[Int]=Array.emptyIntArray): CDFloatArray = reduce( addOp, reduceDims, 0f )
  def mean = sum()/this.getSize
  def maxScaledDiff( other: CDFloatArray ): Float = {
    val mean = this.mean
    val diff = ( this - other )
    val weighted_diff = diff / mean
    weighted_diff.maxMag().getArrayData()(0)
  }

  def augmentFlat( flat_index: FlatIndex, value: Float, opName: String = "add"  ): Unit = {
    val storageIndex: StorageIndex = getIterator.flatToStorage( flat_index )
    setStorageValue( storageIndex, getOp(opName)( getStorageValue( storageIndex ), value ) )
  }
  def augment( coord_indices: Array[Int], value: Float, opName: String = "add"  ): Unit = {
    val storageIndex = cdIndexMap.getStorageIndex( coord_indices )
    setStorageValue( storageIndex,getOp(opName)( getStorageValue( storageIndex ), value ) )
  }
  def map( f: Float => Float ): CDFloatArray = {
    val mappedData: IndexedSeq[Float] = for (i <- (0 until getStorageSize); array_value = getStorageValue(i) ) yield if( valid(array_value) ) { f(array_value) } else { invalid }
    new CDFloatArray( cdIndexMap, FloatBuffer.wrap(mappedData.toArray), invalid )
  }
  def copySectionData( maxValue: Int = Int.MaxValue ): FloatBuffer =  {
    logger.info( " >>>> copySectionData: cap=%d, maxval=%d index=%s".format( floatStorage.capacity(), maxValue, cdIndexMap.toString ) )
    val size = getSize
    if( size == 0 ) {
      FloatBuffer.allocate(0)
    }  else {
      val floatData = for ( index <- getIterator; if index < maxValue ) yield { getValue(index) }
      FloatBuffer.wrap(floatData.toArray)
    }
  }

  def getValue( index: Int ): Float  = {
    try {
      floatStorage.get(index)
    } catch {
      case ex: java.lang.IndexOutOfBoundsException =>
        logger.error( "Index Error in copySectionData at index %d, index size = %d, storage size = %d, shape = [%s], isStorageCongruent = %s".format( index, getSize, getStorageSize, getShape.mkString(","), isStorageCongruent.toString ) )
        throw new Exception( " Invalid array access ")
    }

  }
  def append( other: CDFloatArray ): CDFloatArray = {
    val newIndex = getIndex.append( other.getIndex )
    val new_storage = FloatBuffer.wrap( getStorageArray ++ other.getStorageArray )
    new CDFloatArray( newIndex, new_storage, invalid )
  }
  def weightedReduce( reductionOp: ReduceOpFlt, initVal: Float, accumulation_index: CDIndexMap, weightsOpt: Option[CDFloatArray] = None ): ( CDFloatArray, CDFloatArray ) = {
    val value_accumulator: CDFloatArray = spawn( accumulation_index, initVal )
    val weights_accumulator: CDFloatArray = spawn( accumulation_index, initVal )
    val shape = getShape
    val iter = getIterator
    for (index <- iter; array_value = getStorageValue(index); if valid(array_value); coordIndices = iter.getCoordinateIndices) weightsOpt match {
      case Some(weights) =>
        try {
          val weight = weights.getValue(coordIndices)
          value_accumulator.setValue(coordIndices, reductionOp(value_accumulator.getValue(coordIndices), array_value * weight))
          weights_accumulator.setValue(coordIndices, weights_accumulator.getValue(coordIndices) + weight)
        } catch {
          case ex: Throwable =>
            println( "Error! ")
        }
      case None =>
        value_accumulator.setValue(coordIndices, reductionOp(value_accumulator.getValue(coordIndices), array_value))
        weights_accumulator.setValue(coordIndices, weights_accumulator.getValue(coordIndices) + 1f )
    }
    ( value_accumulator.getReducedArray, weights_accumulator.getReducedArray )
  }

//  def weightedSum( axes: Array[Int], weightsOpt: Option[CDFloatArray] ): ( CDFloatArray, CDFloatArray ) = {
//    val ua = ma2Array(this)
//    val wtsOpt = weightsOpt.map( ma2Array(_) )
//    val wtsIterOpt = wtsOpt.map( _.array.getIndexIterator )
////    val op: ma2Array.ReduceOp = (x,y)=>x+y
//    wtsOpt match {
//      case Some( wts ) => if( !wts.array.getShape.sameElements(getShape) ) { throw new Exception( s"Weights shape [${wts.array.getShape().mkString(",")}] does not match data shape [${getShape.mkString(",")}]") }
//      case None => Unit
//    }
//    val rank = ua.array.getRank
//    val iter: IndexIterator =	ua.array.getIndexIterator()
//    if( axes.length == rank ) {
//      var result = 0f
//      var count = 0f
//      var result_shape = Array.fill[Int](rank)(1)
//      while ( { iter.hasNext }) {
//        val fval = iter.getFloatNext
//        if( ( fval != ua.missing ) && !fval.isNaN ) {
//          wtsIterOpt match {
//            case Some( wtsIter ) =>
//              val wtval = wtsIter.getFloatNext
//              result = result + fval * wtval
//              count = count + wtval
//            case None =>
//              result = result + fval
//              count = count + 1f
//          }
//        }
//      }
//      ( CDFloatArray(result_shape,Array(result),ua.missing), CDFloatArray(result_shape,Array(count),ua.missing) )
//    }
//    else if( axes.length == 1 ) {
//      val target_shape: Array[Int] = getShape.clone
//      target_shape( axes(0) ) = 1
//      val target_array = ma2Array( target_shape, 0.0f, ua.missing )
//      val weights_array = ma2Array( target_shape, 0.0f, ua.missing )
//      val targ_index: Index =	target_array.array.getIndex()
//      while ( { iter.hasNext } ) {
//        val fval = iter.getFloatNext
//        if( ( fval != ua.missing ) && !fval.isNaN ) {
//          var coords: Array[Int] = iter.getCurrentCounter
//          coords( axes(0) ) = 0
//          targ_index.set( coords )
//          val current_index = targ_index.currentElement()
//          wtsIterOpt match {
//            case Some(wtsIter) =>
//              val wtval = wtsIter.getFloatNext
//              target_array.array.setFloat(current_index, target_array.array.getFloat(current_index) + fval*wtval )
//              weights_array.array.setFloat(current_index, weights_array.array.getFloat(current_index) + wtval )
//            case None =>
//              target_array.array.setFloat( current_index, target_array.array.getFloat(current_index) + fval )
//              weights_array.array.setFloat( current_index, weights_array.array.getFloat(current_index) + 1.0f )
//          }
//        }
//      }
//      ( target_array.toCDFloatArray, weights_array.toCDFloatArray )
//    } else {
//      throw new Exception( "Undefined operation")
//    }
//  }

//  def mean( accumulation_index: CDIndexMap, weightsOpt: Option[CDFloatArray] = None): CDFloatArray = {
//    weightedReduce( addOp, 0f, accumulation_index, weightsOpt ) match {
//      case ( values_sum, weights_sum ) =>
//        values_sum / weights_sum
//    }
//  }
//
//  def anomaly( accumulation_index: CDIndexMap, weightsOpt: Option[CDFloatArray] = None ): CDFloatArray = {
//    this - mean( accumulation_index, weightsOpt )
//  }

  def flat_array_square_profile(): CDFloatArray = {
    val cdResult = for (index <- getIterator; value = getStorageValue(index) ) yield value * value
    new CDFloatArray( getShape, FloatBuffer.wrap(cdResult.toArray), invalid )
  }

//  if (weightsOpt.isDefined) assert( getShape.sameElements(weightsOpt.get.getShape), " Weight array in mean op has wrong shape: (%s) vs. (%s)".format( weightsOpt.get.getShape.mkString(","), getShape.mkString(",") ))
//  val values_accumulator: CDFloatArray = getAccumulatorArray(reduceDims, 0f)
//  val weights_accumulator: CDFloatArray = getAccumulatorArray(reduceDims, 0f)
//  val iter = getIterator
//  for (cdIndexMap <- iter; array_value = getData(cdIndexMap); if valid(array_value); coordIndices = iter.getCoordinateIndices) weightsOpt match {
//    case Some(weights) =>
//      val weight = weights.getValue(coordIndices);
//      values_accumulator.setValue(coordIndices, values_accumulator.getValue(coordIndices) + array_value * weight)
//      weights_accumulator.setValue(coordIndices, weights_accumulator.getValue(coordIndices) + weight)
//    case None =>
//      values_accumulator.setValue(coordIndices, values_accumulator.getValue(coordIndices) + array_value)
//      weights_accumulator.setValue(coordIndices, weights_accumulator.getValue(coordIndices) + 1f)
//  }
//  val values_sum: CDFloatArray = values_accumulator.getReducedArray
//  val weights_sum: CDFloatArray = weights_accumulator.getReducedArray

//  def execAccumulatorOp(op: TensorAccumulatorOp, auxDataOpt: Option[CDFloatArray], dimensions: Int*): CDFloatArray = {
//    assert( dimensions.nonEmpty, "Must specify at least one dimension ('axes' arg) for this operation")
//    val filtered_shape: IndexedSeq[Int] = (0 until rank).map(x => if (dimensions.contains(x)) 1 else getShape(x))
//    val slices = auxDataOpt match {
//      case Some(auxData) => Nd4j.concat(0, (0 until filtered_shape.product).map(iS => { Nd4j.create(subset(iS, dimensions: _*).accumulate2( op, auxData.subset( iS, dimensions: _*).tensor )) }): _*)
//      case None =>  Nd4j.concat(0, (0 until filtered_shape.product).map(iS => Nd4j.create(subset(iS, dimensions: _*).accumulate(op))): _*)
//    }
//    new CDFloatArray( slices.reshape(filtered_shape: _* ), invalid )
//  }

//  def mean( weightsOpt: Option[CDFloatArray], dimensions: Int* ): CDFloatArray = execAccumulatorOp( new meanOp(invalid), weightsOpt, dimensions:_* )

  def computeWeights( weighting_type: String, axisDataMap: Map[ Char, ( Int, ma2.Array ) ] ) : CDFloatArray  = {
    weighting_type match {
      case "cosine" =>
        axisDataMap.get('y') match {
          case Some( ( axisIndex, yAxisData ) ) =>
            val axis_length = yAxisData.getSize
            val axis_data = CDFloatArray.factory( yAxisData, Float.MaxValue )
            assert( axis_length == getShape(axisIndex), "Y Axis data mismatch, %d vs %d".format(axis_length,getShape(axisIndex) ) )
            val cosineWeights: CDFloatArray = axis_data.map( x => Math.cos( Math.toRadians(x) ).toFloat )
            val base_shape: Array[Int] = Array( (0 until rank).map(i => if(i==axisIndex) getShape(axisIndex) else 1 ): _* )
            val weightsArray: CDArray[Float] =  CDArray( base_shape, cosineWeights.getStorage, invalid )
            weightsArray.broadcast( getShape )
          case None => throw new NoSuchElementException( "Missing axis data in weights computation, type: %s".format( weighting_type ))
        }
      case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
    }
  }
}

object CDByteArray {
  val bTrue: Byte = 1
  val bFalse: Byte = 0
  def apply( cdIndexMap: CDIndexMap, bytetData: Array[Byte] ): CDByteArray = new CDByteArray(cdIndexMap, ByteBuffer.wrap(bytetData))
  def apply( shape: Array[Int], bytetData: Array[Byte] ): CDByteArray = new CDByteArray( shape, ByteBuffer.wrap(bytetData) )
  def toArray(buffer: ByteBuffer): Array[Byte] = if (buffer.hasArray) buffer.array else { val data = for (index: Int <- (0 until buffer.capacity)) yield buffer.get(index); data.toArray }
}

class CDByteArray( cdIndexMap: CDIndexMap, val byteStorage: ByteBuffer ) extends CDArray[Byte](cdIndexMap,byteStorage) {

  protected def getData: ByteBuffer = byteStorage
  def reduce( reductionOp: CDArray.ReduceOp[Byte], reduceDims: Array[Int], initVal: Byte ): CDArray[Byte] = { throw new Exception( "UNIMPLEMENTED METHOD") }
  def isValid( value: Byte ): Boolean = true

  def this( shape: Array[Int], storage: ByteBuffer ) = this( CDIndexMap(shape,Map.empty[Int,CDCoordMap]), storage )

  def valid( value: Byte ): Boolean = true
  def getInvalid: Byte = Byte.MinValue
  def getStorageValue( index: StorageIndex ): Byte = byteStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Byte ): Unit = byteStorage.put( index, value )

  override def dup(): CDByteArray = new CDByteArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[ByteBuffer] )
  def spawn( shape: Array[Int], fillval: Byte ): CDArray[Byte] = CDArray( shape, ByteBuffer.wrap(Array.fill[Byte]( shape.product )(fillval)), getInvalid  )
  def spawn( index: CDIndexMap, fillval: Byte ): CDArray[Byte] = CDArray( index, ByteBuffer.wrap(Array.fill[Byte]( index.getStorageShape.product )(fillval)), getInvalid  )

  def copySectionData(maxSize: Int = Int.MaxValue): ByteBuffer = {
    val array_data_iter = for (index <- getIterator; if( index<maxSize); value = getStorageValue(index)) yield value
    ByteBuffer.wrap(array_data_iter.toArray)
  }
  def getSectionArray( maxSize: Int = Int.MaxValue ): Array[Byte] = {
    val data = CDByteArray.toArray( getSectionData(maxSize).asInstanceOf[ByteBuffer] )
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }
}

object CDIntArray {
  def toArray(buffer: IntBuffer): Array[Int] = if (buffer.hasArray) buffer.array else { val data = for (index: Int <- (0 until buffer.capacity)) yield buffer.get(index); data.toArray }
}

class CDIntArray( cdIndexMap: CDIndexMap, val intStorage: IntBuffer ) extends CDArray[Int](cdIndexMap,intStorage) {

  protected def getData: IntBuffer = intStorage
  def getStorageValue( index: StorageIndex ): Int = intStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Int ): Unit = intStorage.put( index, value )
//  def reduce( reductionOp: CDArray.ReduceOp[Int], reduceDims: Array[Int], initVal: Int ): CDArray[Int] = { throw new Exception( "UNIMPLEMENTED METHOD") }

  def this( shape: Array[Int], storage: IntBuffer ) = this( CDIndexMap(shape,Map.empty[Int,CDCoordMap]), storage )
  def valid( value: Int ): Boolean = true
  def getInvalid: Int = Int.MinValue

  override def dup(): CDIntArray = new CDIntArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[IntBuffer] )
  def spawn( shape: Array[Int], fillval: Int ): CDArray[Int] = CDArray( shape, IntBuffer.wrap(Array.fill[Int]( shape.product )(fillval)), getInvalid  )
  def spawn( index: CDIndexMap, fillval: Int ): CDArray[Int] = CDArray( index, IntBuffer.wrap(Array.fill[Int]( index.getStorageShape.product )(fillval)), getInvalid  )

  def copySectionData(maxSize: Int = Int.MaxValue): IntBuffer = {
    val array_data_iter = for ( index <- getIterator; if( index<maxSize); value = getStorageValue(index) ) yield value
    IntBuffer.wrap(array_data_iter.toArray)
  }
  def getSectionArray( maxSize: Int = Int.MaxValue ): Array[Int] = {
    val data = CDIntArray.toArray( getSectionData(maxSize).asInstanceOf[IntBuffer] )
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }
}

object CDLongArray {
  def toArray(buffer: LongBuffer): Array[Long] = if (buffer.hasArray) buffer.array else { val data = for (index: Int <- (0 until buffer.capacity)) yield buffer.get(index); data.toArray }
  def apply( shape: Array[Int], longData: Array[Long] ): CDLongArray  = new CDLongArray( shape, LongBuffer.wrap(longData) )

  def factory( array: ucar.ma2.Array ): CDLongArray = {
    val storage = CDLongArray.toLongBuffer( array )
    new CDLongArray( new CDIndexMap(array.getShape), storage )
  }

  def toLongBuffer( array: ucar.ma2.Array ): LongBuffer = array.getElementType.toString match {
    case "float"  => LongBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]].map( _.toLong )  )
    case "double" => LongBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]].map( _.toLong ) )
    case "int"    => LongBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]].map( _.toLong ) )
    case "short"  => LongBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]].map( _.toLong ) )
    case "long"   => LongBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Long]] )
    case x        => LongBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Byte]].map( _.toLong ) )
  }
}

class CDLongArray( cdIndexMap: CDIndexMap, val longStorage: LongBuffer ) extends CDArray[Long](cdIndexMap,longStorage) {

  protected def getData: LongBuffer = longStorage
  def getStorageValue( index: StorageIndex ): Long = longStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Long ): Unit = longStorage.put( index, value )
//  def reduce( reductionOp: CDArray.ReduceOp[Long], reduceDims: Array[Int], initVal: Long ): CDArray[Long] = { throw new Exception( "UNIMPLEMENTED METHOD") }

  def this( shape: Array[Int], storage: LongBuffer ) = this( CDIndexMap(shape,Map.empty[Int,CDCoordMap]), storage )
  def valid( value: Long ): Boolean = true
  def getInvalid: Long = Long.MinValue
  def getStorageArray: Array[Long] = CDLongArray.toArray( longStorage )

  override def dup(): CDLongArray = new CDLongArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[LongBuffer] )
  def spawn( shape: Array[Int], fillval: Long ): CDArray[Long] = CDArray( shape, LongBuffer.wrap(Array.fill[Long]( shape.product )(fillval)), getInvalid  )
  def spawn( index: CDIndexMap, fillval: Long ): CDArray[Long] = CDArray( index, LongBuffer.wrap(Array.fill[Long]( index.getStorageShape.product )(fillval)), getInvalid  )

  def copySectionData(maxSize: Int = Int.MaxValue): LongBuffer = {
    val array_data_iter = for ( index <- getIterator; if( index<maxSize); value = getStorageValue(index) ) yield value
    LongBuffer.wrap(array_data_iter.toArray)
  }
  def getSectionArray( maxSize: Int = Int.MaxValue ): Array[Long] = {
    val data = CDLongArray.toArray( getSectionData(maxSize).asInstanceOf[LongBuffer] )
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }
  def append( other: CDLongArray ): CDLongArray = {
    val newIndex = getIndex.append( other.getIndex )
    val new_storage = LongBuffer.wrap( getStorageArray ++ other.getStorageArray )
    new CDLongArray( newIndex, new_storage )
  }
  def getArrayData(maxSize: Int = Int.MaxValue): Array[Long]  = {
    val data = if( isStorageCongruent ) getStorageArray else getSectionArray(maxSize)
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }

}
object CDShortArray {
  def toArray(buffer: ShortBuffer): Array[Short] = if (buffer.hasArray) buffer.array else { val data = for (index: Int <- (0 until buffer.capacity)) yield buffer.get(index); data.toArray }
}

class CDShortArray( cdIndexMap: CDIndexMap, val shortStorage: ShortBuffer ) extends CDArray[Short](cdIndexMap,shortStorage) {

  protected def getData: ShortBuffer = shortStorage
  def getInvalid: Short = Short.MinValue
  def getStorageValue( index: StorageIndex ): Short = shortStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Short ): Unit = shortStorage.put( index, value )
//  def reduce( reductionOp: CDArray.ReduceOp[Short], reduceDims: Array[Int], initVal: Short ): CDArray[Short] = { throw new Exception( "UNIMPLEMENTED METHOD") }

  def this( shape: Array[Int], storage: ShortBuffer ) = this( CDIndexMap(shape,Map.empty[Int,CDCoordMap]), storage )
  def valid( value: Short ): Boolean = true

  override def dup(): CDShortArray = new CDShortArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[ShortBuffer] )
  def spawn( shape: Array[Int], fillval: Short ): CDArray[Short] = CDArray( shape, ShortBuffer.wrap(Array.fill[Short]( shape.product )(fillval)), getInvalid  )
  def spawn( index: CDIndexMap, fillval: Short ): CDArray[Short] = CDArray( index, ShortBuffer.wrap(Array.fill[Short]( index.getStorageShape.product )(fillval)), getInvalid  )

  def copySectionData(maxSize: Int = Int.MaxValue): ShortBuffer = {
    val array_data_iter = for ( index <- getIterator; if( index<maxSize); value = getStorageValue(index) ) yield value
    ShortBuffer.wrap(array_data_iter.toArray)
  }
  def getSectionArray( maxSize: Int = Int.MaxValue ): Array[Short] = {
    val data = CDShortArray.toArray( getSectionData(maxSize).asInstanceOf[ShortBuffer] )
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }
}

object CDDoubleArray {
  type ReduceOpDbl = CDArray.ReduceOp[Double]
  implicit def cdArrayConverter(target: CDArray[Double]): CDDoubleArray = new CDDoubleArray(target.getIndex, target.getStorage.asInstanceOf[DoubleBuffer], target.getInvalid )
  implicit def toUcarArray(target: CDDoubleArray): ma2.Array = ma2.Array.factory( ma2.DataType.DOUBLE, target.getShape, target.getSectionData().array() )

  def apply( shape: Array[Int], dblData: Array[Double], invalid: Double ): CDDoubleArray  = new CDDoubleArray( shape, DoubleBuffer.wrap(dblData),  invalid )

  def toDoubleBuffer( array: ucar.ma2.Array ): DoubleBuffer = array.getElementType.toString match {
    case "float"  => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]].map( _.toDouble )  )
    case "double" => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]] )
    case "int"    => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]].map( _.toDouble ) )
    case "short"  => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]].map( _.toDouble ) )
    case "long"   => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Long]].map( _.toDouble )  )
    case x        => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Byte]].map( _.toDouble ) )
  }
  def toArray(buffer: DoubleBuffer): Array[Double] = if( buffer.hasArray ) buffer.array else { val data =for( index: Int <- (0 until buffer.capacity) ) yield buffer.get( index ); data.toArray }

  def toDoubleBuffer( buffer: Buffer ): DoubleBuffer = buffer match {
    case x: DoubleBuffer  => buffer.asInstanceOf[ DoubleBuffer ]
    case x => throw new Exception( "Attempt to convert non-float buffer to DoubleBuffer")
  }

  def toDoubleArray( array: ucar.ma2.Array ): Array[Double] = array.getElementType.toString match {
    case "float"  => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]].map( _.toDouble )
    case "double" => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]]
    case "int"    => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]].map( _.toDouble )
    case "long"   => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Long]].map( _.toDouble )
    case "short"  => array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]].map( _.toDouble )
  }

  def factory( array: ucar.ma2.Array, invalid: Double = Double.MaxValue ): CDDoubleArray = {
    val storage = CDDoubleArray.toDoubleBuffer( array )
    new CDDoubleArray(new CDIndexMap(array.getShape), storage, invalid )
  }

  def combine( reductionOp: ReduceOpDbl, input0: CDDoubleArray, input1: CDDoubleArray ): CDDoubleArray = {
    val sameStructure = input0.getStride.sameElements(input1.getStride)
    val iter = MultiArrayIterator(input0,input1)
    val result = for (flatIndex <- iter; values = iter.values; v0 = values.head; v1 = values.last ) yield {
      if( (v0 == iter.invalid) || (v1 == iter.invalid) ) iter.invalid
      else reductionOp( v0, v1 )
    }
    new CDDoubleArray(iter.getShape, DoubleBuffer.wrap(result.toArray), input0.invalid)
  }

}

class CDDoubleArray( cdIndexMap: CDIndexMap, val doubleStorage: DoubleBuffer, protected val invalid: Double ) extends CDArray[Double](cdIndexMap,doubleStorage) {

  protected def getData: DoubleBuffer = doubleStorage
  def getInvalid = invalid
  def getStorageValue( index: StorageIndex ): Double = doubleStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Double ): Unit = doubleStorage.put( index, value )
//  def reduce( reductionOp: CDArray.ReduceOp[Double], reduceDims: Array[Int], initVal: Double ): CDArray[Double] = { throw new Exception( "UNIMPLEMENTED METHOD") }

  def this( shape: Array[Int], storage: DoubleBuffer, invalid: Double ) = this( CDIndexMap(shape,Map.empty[Int,CDCoordMap]), storage, invalid )
  def toUcarArray: ma2.Array = ma2.Array.factory(ma2.DataType.DOUBLE, getShape, getSectionData() )

  def valid( value: Double ) = ( value != invalid )

  override def dup(): CDDoubleArray = new CDDoubleArray( cdIndexMap.getShape, this.getSectionData(), getInvalid )
  def spawn( shape: Array[Int], fillval: Double ): CDArray[Double] = CDArray( shape, DoubleBuffer.wrap(Array.fill[Double]( shape.product )(fillval)), getInvalid )
  def spawn( index: CDIndexMap, fillval: Double ): CDArray[Double] = CDArray( index, DoubleBuffer.wrap(Array.fill[Double]( index.getStorageShape.product )(fillval)), getInvalid )

  def copySectionData(maxSize: Int = Int.MaxValue): DoubleBuffer = {
    val array_data_iter = for ( index <- getIterator; if( index<maxSize); value = getStorageValue(index) ) yield value
    DoubleBuffer.wrap(array_data_iter.toArray)
  }

  def zeros: CDDoubleArray = new CDDoubleArray( getShape, DoubleBuffer.wrap(Array.fill[Double]( getSize )(0)), invalid )
  def invalids: CDDoubleArray = new CDDoubleArray( getShape, DoubleBuffer.wrap(Array.fill[Double]( getSize )(invalid)), invalid )

  override def getSectionData(maxCopySize: Int = Int.MaxValue): DoubleBuffer = super.getSectionData(maxCopySize).asInstanceOf[DoubleBuffer]
  def getStorageData: DoubleBuffer = doubleStorage
  def getStorageArray: Array[Double] = CDDoubleArray.toArray( doubleStorage )
  def getSectionArray(maxSize: Int = Int.MaxValue): Array[Double] = {
    val data = CDDoubleArray.toArray( getSectionData(maxSize) )
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }
  def getArrayData(maxSize: Int = Int.MaxValue): Array[Double]  = {
    val data = if( isStorageCongruent ) getStorageArray else getSectionArray(maxSize)
    if( maxSize < data.length ) { data.slice(0,maxSize) } else { data }
  }

  def append( other: CDDoubleArray ): CDDoubleArray = {
    val newIndex = getIndex.append( other.getIndex )
    val new_storage = DoubleBuffer.wrap( getStorageArray ++ other.getStorageArray )
    new CDDoubleArray( newIndex, new_storage, invalid )
  }
}

//
//object ArrayTest extends App {
//  val base_shape = Array(5,5,5)
//  val subset_origin = Array(1,1,1)
//  val subset_shape = Array(2,2,2)
//  val storage = Array.iterate( 0f, 125 )( x => x + 1f )
//  val cdIndexMap: CDIndexMap = CDIndexMap.factory( base_shape )
//  val cd_array = CDArray( cdIndexMap, storage, Float.MaxValue )
//  val ma2_array: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, base_shape, storage )
//  val cd_array_subset = cd_array.section( subset_origin,  subset_shape )
//  val ma2_array_subset = ma2_array.sectionNoReduce( subset_origin,  subset_shape, Array(1,1,1) )
//
//  val cd_array_slice = cd_array_subset.slice( 0, 0 )
//  val ma2_array_slice = ma2_array_subset.slice( 0, 0 ).reshape( Array(1,2,2) )
//
//  val cd_array_tp = cd_array_slice.transpose( 1,2 )
//  val ma2_array_tp = ma2_array_slice.transpose( 1,2 )
//
//
//  println( cd_array_tp.toString )
//  println( ma2_array_tp.toString )
//
////  val cd_array_bcast = cd_array_slice.broadcast( 0, 3 )
////  println( cd_array_bcast.toString )
//}
//
//object ArrayPerformanceTest extends App {
//  import scala.collection.mutable.ListBuffer
//  val base_shape = Array(100,100,10)
//  val storage: Array[Float] = Array.iterate( 0f, 100000 )( x => x + 1f )
//  val cdIndexMap: CDIndexMap = CDIndexMap.factory( base_shape )
//  val cd_array = new CDFloatArray( cdIndexMap, storage, Float.MaxValue )
//  val ma2_array: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, base_shape, storage )
//  val ma2Iter: ma2.IndexIterator = ma2_array.getIndexIterator
//
//  val itest = 2
//
//  itest match {
//    case 0 =>
//      val t00 = System.nanoTime
//      val cdResult = cd_array.flat_array_square_profile()
//      val t01 = System.nanoTime
//      println ("cd2Result Time = %.4f,  ".format ((t01 - t00) / 1.0E9) )
//
//      var result = new ListBuffer[Float] ()
//      val t10 = System.nanoTime
//      val ma2Result = while (ma2Iter.hasNext) { val value = ma2Iter.getFloatNext; result += value * value }
//      val ma2ResultData = result.toArray
//      val t11 = System.nanoTime
//      println ("ma2Result Time = %.4f,  ".format ((t11 - t10) / 1.0E9) )
//
//    case 1 =>
//      val section_origin = Array( 10, 10, 1 )
//      val section_shape = Array( 80, 80, 8 )
//      val section_strides = Array( 1, 1, 1 )
//
//      val ma2Result = ma2_array.section( section_origin, section_shape )
//      val t10 = System.nanoTime
//      val ma2Data = ma2Result.getDataAsByteBuffer
//      val t11 = System.nanoTime
//
//      println ("ma2Result Time = %.4f,  ".format ((t11 - t10) / 1.0E9) )
//
//    case 2 =>
//      val section_origin = Array( 10, 10, 1 )
//      val section_shape = Array( 80, 80, 8 )
//      val section_strides = Array( 1, 1, 1 )
//
//      val cdResult = cd_array.section( section_origin, section_shape )
//      val t00 = System.nanoTime
//      val cdData = cdResult.getDataAsByteBuffer
//      val t01 = System.nanoTime
//
//      println ("cd2Result Time = %.4f,  ".format ((t01 - t00) / 1.0E9) )
//
//  }
//
//
//
//}
//
//
//
//
//
//
