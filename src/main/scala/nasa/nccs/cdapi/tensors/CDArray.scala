// Based on ucar.ma2.Array, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

package nasa.nccs.cdapi.tensors
import java.nio._
import nasa.nccs.cdapi.tensors.CDArray.{FlatIndex, StorageIndex}
import nasa.nccs.utilities.{Loggable, cdsutils}
import ucar.ma2
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CDArray {

  type ReduceOp[T] = (T,T)=>T
  type StorageIndex = Int
  type FlatIndex = Int

  def factory[T <: AnyVal]( shape: Array[Int], storage: Buffer, invalid: T ): CDArray[T] = factory( new CDIndexMap(shape), storage, invalid )

  def factory[T <: AnyVal]( index: CDIndexMap, storage: Buffer, invalid: T ): CDArray[T] = {
    storage match {
      case x: FloatBuffer => new CDFloatArray( index, storage.asInstanceOf[FloatBuffer], invalid.asInstanceOf[Float] ).asInstanceOf[CDArray[T]]
      case x: IntBuffer => new CDIntArray( index, storage.asInstanceOf[IntBuffer] ).asInstanceOf[CDArray[T]]
      case x: ByteBuffer => new CDByteArray( index, storage.asInstanceOf[ByteBuffer] ).asInstanceOf[CDArray[T]]
      case x: ShortBuffer => new CDShortArray( index, storage.asInstanceOf[ShortBuffer] ).asInstanceOf[CDArray[T]]
      case x: DoubleBuffer => new CDDoubleArray( index, storage.asInstanceOf[DoubleBuffer], invalid.asInstanceOf[Double] ).asInstanceOf[CDArray[T]]
      case x => throw new Exception( "Unsupported elem type in CDArray: " + x)
    }
  }

  def toBuffer( array: ucar.ma2.Array ): Buffer = array.getElementType.toString match {
    case "float"  => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]] )
    case "double" => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]] )
    case "int"    => IntBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]] )
    case "short"  => ShortBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]] )
    case x                => ByteBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Byte]] )
  }

  def factory[T <: AnyVal]( array: ucar.ma2.Array, invalid: T ): CDArray[T] =
    factory( new CDIndexMap( array.getShape ), toBuffer( array), invalid )

  def getDataType( storage: Buffer ): ma2.DataType = storage match {
    case x: DoubleBuffer => ma2.DataType.DOUBLE
    case x: FloatBuffer => ma2.DataType.FLOAT
    case x: IntBuffer => ma2.DataType.INT
    case x: ShortBuffer => ma2.DataType.SHORT
    case x: ByteBuffer => ma2.DataType.BYTE
  }


}

abstract class CDArray[ T <: AnyVal ]( private val cdIndexMap: CDIndexMap, private val storage: Buffer ) extends Loggable {
  protected val rank = cdIndexMap.getRank
  protected val dataType = CDArray.getDataType( storage )

  def isStorageCongruent: Boolean = ( cdIndexMap.getSize == getStorageSize ) && !cdIndexMap.broadcasted
  def getInvalid: T
  def getStorageSize: Int = storage.capacity
  def getElementSize: Int = dataType.getSize
  def getIndex: CDIndexMap = new CDIndexMap( this.cdIndexMap )
  def dup() = CDArray.factory[T]( cdIndexMap, storage, getInvalid )
  def getIterator: CDIterator = if(isStorageCongruent) new CDStorageIndexIterator( cdIndexMap ) else CDIterator.factory( cdIndexMap )
  def getRank: Int = rank
  def getShape: Array[Int] = cdIndexMap.getShape
  def getStride: Array[Int] = cdIndexMap.getStride
  def getOffset: Int = cdIndexMap.getOffset
  def getReducedShape: Array[Int] = cdIndexMap.getReducedShape
  def getStorageValue( index: StorageIndex ): T
  def getValue( indices: Array[Int] ): T = getStorageValue( cdIndexMap.getStorageIndex(indices) )
  def getFlatValue( index: FlatIndex ): T = getStorageValue( getIterator.flatToStorage(index) )
  def setStorageValue( index: StorageIndex, value: T ): Unit
  def setValue( indices: Array[Int], value: T ): Unit = { setStorageValue( cdIndexMap.getStorageIndex(indices), value )   }
  def setFlatValue( index: FlatIndex, value: T  ): Unit = setStorageValue( getIterator.flatToStorage(index), value )
  def getSize: Int =  cdIndexMap.getSize
  def getSizeBytes: Int =  cdIndexMap.getSize * getElementSize
  def getRangeIterator(ranges: List[ma2.Range] ): CDIterator = section(ranges).getIterator
  def getStorage: Buffer = storage
  def copySectionData( maxSize: Int = Int.MaxValue ): Buffer
  def getSectionData( maxSize: Int = Int.MaxValue ): Buffer = if( isStorageCongruent ) getStorage else copySectionData(maxSize)
  def section( subsection: ma2.Section ): CDArray[T] = section( subsection.getRanges.toList )
  def section(ranges: List[ma2.Range]): CDArray[T] = createView(cdIndexMap.section(ranges))
  def valid( value: T ): Boolean
  def spawn( shape: Array[Int], fillval: T ): CDArray[T]
  def sameShape( cdIndex: CDIndexMap ): Boolean = cdIndexMap.getShape.sameElements( cdIndex.getShape )
  def sameStorage( cdIndex: CDIndexMap ): Boolean = ( cdIndexMap.getStride.sameElements( cdIndex.getStride ) && ( cdIndexMap.getOffset == cdIndex.getOffset ) )
  def sameShape[R <: AnyVal]( array: CDArray[R] ): Boolean = sameShape( array.getIndex )
  def sameStorage[R <: AnyVal]( array: CDArray[R] ): Boolean = sameStorage( array.getIndex )

  def getAccumulatorArray( reduceAxes: Array[Int], fillval: T, fullShape: Array[Int] = getShape ): CDArray[T] = {
    val reducedShape = for( idim <- ( 0 until rank) ) yield if( reduceAxes.contains(idim) ) 1 else fullShape( idim )
    val accumulator = spawn( reducedShape.toArray, fillval )
    accumulator.broadcast( fullShape )
  }

  def getReducedArray: CDArray[T] = { CDArray.factory[T]( getReducedShape, storage, getInvalid ) }

  def reduce( reductionOp: CDArray.ReduceOp[T], reduceDims: Array[Int], initVal: T, coordMapOpt: Option[CDCoordMap] = None ): CDArray[T] = {
    val fullShape = coordMapOpt match { case Some(coordMap) => coordMap.mapShape( getShape ); case None => getShape }
    val accumulator: CDArray[T] = getAccumulatorArray( reduceDims, initVal, fullShape )
    val iter = getIterator
    coordMapOpt match {
      case Some(coordMap) =>
        for (index <- iter; array_value = getStorageValue(index); if valid(array_value); coordIndices = iter.getCoordinateIndices) {
          val mappedCoords = coordMap.map(coordIndices)
          accumulator.setValue(mappedCoords, reductionOp(accumulator.getValue(mappedCoords), array_value))
        }
      case None =>
        for (index <- iter; array_value = getStorageValue(index); coordIndices = iter.getCoordinateIndices) {
          if( valid(array_value) ) {
            val reduced_value = reductionOp(accumulator.getValue(coordIndices), array_value)
            accumulator.setValue(coordIndices, reduced_value)
          }
        }
    }
    accumulator.getReducedArray
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

  def section(origin: Array[Int], shape: Array[Int], strideOpt: Option[Array[Int]]=None): CDArray[T] =
    createView(cdIndexMap.section(createRanges(origin,shape,strideOpt)))

  def slice(dim: Int, value: Int): CDArray[T] = {
    val origin: Array[Int] = new Array[Int](rank)
    val shape: Array[Int] = getShape
    origin(dim) = value
    shape(dim) = 1
    section(origin, shape)
  }

  def broadcast(dim: Int, size: Int ): CDArray[T] = createView( cdIndexMap.broadcast(dim,size) )
  def broadcast(shape: Array[Int] ): CDArray[T] = createView( cdIndexMap.broadcast(shape) )
  def flip(dim: Int): CDArray[T] = createView(cdIndexMap.flip(dim))
  def transpose(dim1: Int, dim2: Int): CDArray[T] = createView(cdIndexMap.transpose(dim1, dim2))
  def permute(dims: Array[Int]): CDArray[T] = createView(cdIndexMap.permute(dims))

  def reshape(shape: Array[Int]): CDArray[T] = {
    if( shape.product != getSize ) throw new IllegalArgumentException("reshape arrays must have same total size")
    CDArray.factory( new CDIndexMap(shape), getSectionData(), getInvalid )
  }

  def reduce: CDArray[T] = {
    val ri: CDIndexMap = cdIndexMap.reduce
    if (ri eq cdIndexMap) return this
    createView(ri)
  }

  def reduce(dim: Int): CDArray[T] = createView(cdIndexMap.reduce(dim))
  def isVlen: Boolean = false

  protected def createView( cdIndexMap: CDIndexMap ): CDArray[T] = CDArray.factory(cdIndexMap, storage, getInvalid )


  override def toString: String = "Index: " + this.cdIndexMap.toString

}

object CDFloatArray {
  type ReduceOpFlt = CDArray.ReduceOp[Float]
  implicit def cdArrayConverter( target: CDArray[Float] ): CDFloatArray = new CDFloatArray( target.getIndex, target.getStorage.asInstanceOf[ FloatBuffer ], target.getInvalid )
  implicit def toUcarArray( target: CDFloatArray ): ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, target.getShape, target.getSectionData().array() )
  val bTrue: Byte = 1
  val bFalse: Byte = 0

  def apply( cdIndexMap: CDIndexMap, floatData: Array[Float], invalid: Float ): CDFloatArray  = new CDFloatArray( cdIndexMap, FloatBuffer.wrap(floatData),  invalid )
  def apply( shape: Array[Int], floatData: Array[Float], invalid: Float ): CDFloatArray  = new CDFloatArray( shape, FloatBuffer.wrap(floatData),  invalid )
  def apply( target: CDArray[Float] ): CDFloatArray  = CDFloatArray.cdArrayConverter( target )

  def toFloatBuffer( array: ucar.ma2.Array ): FloatBuffer = array.getElementType.toString match {
    case "float"  => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]] )
    case "double" => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]].map( _.toFloat ) )
    case "int"    => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]].map( _.toFloat ) )
    case "short"  => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]].map( _.toFloat ) )
    case x        => FloatBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Byte]].map( _.toFloat ) )
  }

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
    val new_array: CDFloatArray = CDArray.factory( shape, FloatBuffer.wrap( Array.fill[Float]( shape.product )(0f) ), invalid  )
    val cdIndexMap = new CDIndexMap( shape )
    val iter = CDIterator.factory( cdIndexMap )
    for ( index <- iter; coords = iter.getCoordinateIndices; value = f(coords) ) new_array.setValue( coords, value )
    new_array
  }

  def combine( reductionOp: ReduceOpFlt, input0: CDFloatArray, input1: CDFloatArray ): CDFloatArray = {
    val sameStructure = input0.getStride.sameElements(input1.getStride)
    val iter = DualArrayIterator(input0,input1)
    val result = for (flatIndex <- iter; v0 = iter.value0; v1 = iter.value1 ) yield
        if (!input0.valid(v0)) input0.invalid else if (!input1.valid(v1)) input0.invalid else reductionOp(v0, v1)
    new CDFloatArray( iter.getShape, FloatBuffer.wrap(result.toArray), input0.invalid )
  }

  def accumulate( reductionOp: ReduceOpFlt, input0: CDFloatArray, input1: CDFloatArray ): Unit = {
    val sameStructure = input0.getStride.sameElements(input1.getStride)
    val iter = DualArrayIterator(input0,input1)
    for (flatIndex <- iter;  v0 = iter.value0; if (input0.valid(v0)); v1 = iter.value1; if (input1.valid(v1))) {
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
  type ReduceOpFlt = CDFloatArray.ReduceOpFlt
  val addOp: ReduceOpFlt = (x:Float, y:Float) => ( x + y )
  val subtractOp: ReduceOpFlt = (x:Float, y:Float) => ( x - y )
  val multiplyOp: ReduceOpFlt = (x:Float, y:Float) => ( x * y )
  val divideOp: ReduceOpFlt = (x:Float, y:Float) => ( x / y )
  val maxOp: ReduceOpFlt = (x:Float, y:Float) => ( if( x > y ) x else y )
  val minOp: ReduceOpFlt = (x:Float, y:Float) => ( if( x < y ) x else y )
  val eqOp: ReduceOpFlt = (x:Float, y:Float) => ( y )
  def getOp( opName: String ): ReduceOpFlt = opName match {
    case x if x.startsWith("sum") => addOp
    case x if x.startsWith("add") => addOp
    case x if x.startsWith("sub") => subtractOp
    case x if x.startsWith("mul") => multiplyOp
    case x if x.startsWith("div") => divideOp
    case x if x.startsWith("max") => maxOp
    case x if x.startsWith("min") => minOp
  }
  def getStorageValue( index: StorageIndex ): Float = floatStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Float ): Unit = floatStorage.put( index, value )
  def this( shape: Array[Int], storage: FloatBuffer, invalid: Float ) = this( CDIndexMap.factory(shape), storage, invalid )
  def this( storage: FloatBuffer, invalid: Float ) = this( CDIndexMap.factory( Array(storage.capacity()) ), storage, invalid )
  protected def getData: FloatBuffer = floatStorage
  override def getSectionData( maxSize: Int = Int.MaxValue ): FloatBuffer = super.getSectionData(maxSize).asInstanceOf[FloatBuffer]
  def getStorageData: FloatBuffer = floatStorage
  def isMapped: Boolean = !floatStorage.hasArray
  def getStorageArray: Array[Float] = CDFloatArray.toArray( floatStorage )
  def getSectionArray( maxSize: Int = Int.MaxValue ): Array[Float] = CDFloatArray.toArray( getSectionData(maxSize) )
  def getArrayData( maxSize: Int = Int.MaxValue ): Array[Float]  = if( isStorageCongruent ) getStorageArray else getSectionArray( maxSize )
  override def dup(): CDFloatArray = new CDFloatArray( cdIndexMap.getShape, this.getSectionData(), invalid )
  def valid( value: Float ) = ( value != invalid )
  def toCDFloatArray( target: CDArray[Float] ) = new CDFloatArray( target.getIndex, target.getStorage.asInstanceOf[FloatBuffer], invalid )
  def spawn( shape: Array[Int], fillval: Float ): CDArray[Float] = CDArray.factory( shape, FloatBuffer.wrap(Array.fill[Float]( shape.product )(fillval)), invalid  )
  def zeros: CDFloatArray = new CDFloatArray( getShape, FloatBuffer.wrap( Array.fill[Float]( getSize )(0) ), invalid )
  def invalids: CDFloatArray = new CDFloatArray( getShape, FloatBuffer.wrap( Array.fill[Float]( getSize )(invalid) ), invalid )
  def getInvalid = invalid

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

  def max(reduceDims: Array[Int]): CDFloatArray = reduce( maxOp, reduceDims, -Float.MaxValue )
  def min(reduceDims: Array[Int]): CDFloatArray = reduce( minOp, reduceDims, Float.MaxValue )
  def sum(reduceDims: Array[Int]): CDFloatArray = reduce( addOp, reduceDims, 0f )

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
    val floatData = ( for ( index <- getIterator; if(index<maxValue); value = floatStorage.get(index) ) yield { value } );
    FloatBuffer.wrap(floatData.toArray)
  }
  def merge( other: CDFloatArray ): CDFloatArray = {
    assert( !isMapped && !other.isMapped, "Attempt to merge a mapped array: Not supported.")
    val (a0, a1)  = (dup(), other.dup())
    val newIndex = a0.getIndex.append( a1.getIndex )
    val new_storage = FloatBuffer.wrap( a0.getStorageArray ++ a1.getStorageArray )
    new CDFloatArray( newIndex, new_storage, invalid )
  }
  def weightedReduce( reductionOp: ReduceOpFlt, reduceDims: Array[Int], initVal: Float, weightsOpt: Option[CDFloatArray] = None, coordMapOpt: Option[CDCoordMap] = None ): ( CDFloatArray, CDFloatArray ) = {
    val fullShape = coordMapOpt match { case Some(coordMap) => coordMap.mapShape( getShape ); case None => getShape }
    val bcastReductionDims = coordMapOpt match { case None => reduceDims; case Some( coordMap ) => reduceDims.filterNot( _ == coordMap.dimIndex ) }
    val value_accumulator: CDFloatArray = getAccumulatorArray( bcastReductionDims, initVal, fullShape )
    val weights_accumulator: CDFloatArray = getAccumulatorArray( bcastReductionDims, 0f, fullShape )
    val iter = getIterator
    coordMapOpt match {
      case Some(coordMap) =>
        for (index <- iter; array_value = getStorageValue(index); if valid(array_value); coordIndices = iter.getCoordinateIndices) weightsOpt match {
          case Some(weights) =>
            val weight = weights.getValue(coordIndices);
            val mappedCoords = coordMap.map(coordIndices)
            value_accumulator.setValue(mappedCoords, reductionOp(value_accumulator.getValue(mappedCoords), array_value * weight ))
            weights_accumulator.setValue(mappedCoords, weights_accumulator.getValue(mappedCoords) + weight)
          case None =>
            val mappedCoords = coordMap.map(coordIndices)
            value_accumulator.setValue(mappedCoords, reductionOp(value_accumulator.getValue(mappedCoords), array_value))
            weights_accumulator.setValue(mappedCoords, weights_accumulator.getValue(mappedCoords) + 1f)
        }
      case None =>
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
    }
    ( value_accumulator.getReducedArray, weights_accumulator.getReducedArray )
  }

  def mean(reduceDims: Array[Int], weightsOpt: Option[CDFloatArray] = None): CDFloatArray = {
    weightedReduce( addOp, reduceDims, 0f, weightsOpt ) match {
      case ( values_sum, weights_sum ) =>
        values_sum / weights_sum
    }
  }

  def anomaly( reduceDims: Array[Int], weightsOpt: Option[CDFloatArray] = None ): CDFloatArray = {
    this - mean( reduceDims, weightsOpt )
  }

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
            val weightsArray: CDArray[Float] =  CDArray.factory( base_shape, cosineWeights.getStorage, invalid )
            weightsArray.broadcast( getShape )
          case None => throw new NoSuchElementException( "Missing axis data in weights computation, type: %s".format( weighting_type ))
        }
      case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
    }
  }
  def toDataString: String = "Index: " + this.cdIndexMap.toString + "\n Data = " + mkDataString("[ ",", "," ]")
  def mkDataString( sep: String ): String = getSectionArray().map( _.toString ).mkString( sep )
  def mkDataString( start: String, sep: String, end: String ): String = getSectionArray().map( _.toString ).mkString( start, sep, end )
  def mkBoundedDataString( sep: String, maxSize: Int ): String = getSectionArray(maxSize).map( _.toString ).mkString( sep )
  def mkBoundedDataString( start: String, sep: String, end: String, maxSize: Int ): String = getSectionArray(maxSize).map( _.toString ).mkString( start, sep, end )
}

object CDByteArray {
  val bTrue: Byte = 1
  val bFalse: Byte = 0
  def apply( cdIndexMap: CDIndexMap, bytetData: Array[Byte] ): CDByteArray = new CDByteArray(cdIndexMap, ByteBuffer.wrap(bytetData))
  def apply( shape: Array[Int], bytetData: Array[Byte] ): CDByteArray = new CDByteArray( shape, ByteBuffer.wrap(bytetData) )
}

class CDByteArray( cdIndexMap: CDIndexMap, val byteStorage: ByteBuffer ) extends CDArray[Byte](cdIndexMap,byteStorage) {

  protected def getData: ByteBuffer = byteStorage

  def this( shape: Array[Int], storage: ByteBuffer ) = this( CDIndexMap.factory(shape), storage )

  def valid( value: Byte ): Boolean = true
  def getInvalid: Byte = Byte.MinValue
  def getStorageValue( index: StorageIndex ): Byte = byteStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Byte ): Unit = byteStorage.put( index, value )

  override def dup(): CDByteArray = new CDByteArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[ByteBuffer] )
  def spawn( shape: Array[Int], fillval: Byte ): CDArray[Byte] = CDArray.factory( shape, ByteBuffer.wrap(Array.fill[Byte]( shape.product )(fillval)), getInvalid  )

  def copySectionData(maxSize: Int = Int.MaxValue): ByteBuffer = {
    val array_data_iter = for (index <- getIterator; if( index<maxSize); value = getStorageValue(index)) yield value
    ByteBuffer.wrap(array_data_iter.toArray)
  }
}

class CDIntArray( cdIndexMap: CDIndexMap, val intStorage: IntBuffer ) extends CDArray[Int](cdIndexMap,intStorage) {

  protected def getData: IntBuffer = intStorage
  def getStorageValue( index: StorageIndex ): Int = intStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Int ): Unit = intStorage.put( index, value )

  def this( shape: Array[Int], storage: IntBuffer ) = this( CDIndexMap.factory(shape), storage )
  def valid( value: Int ): Boolean = true
  def getInvalid: Int = Int.MinValue

  override def dup(): CDIntArray = new CDIntArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[IntBuffer] )
  def spawn( shape: Array[Int], fillval: Int ): CDArray[Int] = CDArray.factory( shape, IntBuffer.wrap(Array.fill[Int]( shape.product )(fillval)), getInvalid  )

  def copySectionData(maxSize: Int = Int.MaxValue): IntBuffer = {
    val array_data_iter = for ( index <- getIterator; if( index<maxSize); value = getStorageValue(index) ) yield value
    IntBuffer.wrap(array_data_iter.toArray)
  }
}

class CDShortArray( cdIndexMap: CDIndexMap, val shortStorage: ShortBuffer ) extends CDArray[Short](cdIndexMap,shortStorage) {

  protected def getData: ShortBuffer = shortStorage
  def getInvalid: Short = Short.MinValue
  def getStorageValue( index: StorageIndex ): Short = shortStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Short ): Unit = shortStorage.put( index, value )

  def this( shape: Array[Int], storage: ShortBuffer ) = this( CDIndexMap.factory(shape), storage )
  def valid( value: Short ): Boolean = true

  override def dup(): CDShortArray = new CDShortArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[ShortBuffer] )
  def spawn( shape: Array[Int], fillval: Short ): CDArray[Short] = CDArray.factory( shape, ShortBuffer.wrap(Array.fill[Short]( shape.product )(fillval)), getInvalid  )

  def copySectionData(maxSize: Int = Int.MaxValue): ShortBuffer = {
    val array_data_iter = for ( index <- getIterator; if( index<maxSize); value = getStorageValue(index) ) yield value
    ShortBuffer.wrap(array_data_iter.toArray)
  }
}

object CDDoubleArray {
  implicit def cdArrayConverter(target: CDArray[Double]): CDDoubleArray = new CDDoubleArray(target.getIndex, target.getStorage.asInstanceOf[DoubleBuffer], target.getInvalid )
  implicit def toUcarArray(target: CDDoubleArray): ma2.Array = ma2.Array.factory(ma2.DataType.DOUBLE, target.getShape, target.getSectionData())

  def toDoubleBuffer( array: ucar.ma2.Array ): DoubleBuffer = array.getElementType.toString match {
    case "float"  => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]].map( _.toDouble )  )
    case "double" => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]] )
    case "int"    => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]].map( _.toDouble ) )
    case "short"  => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Short]].map( _.toDouble ) )
    case x        => DoubleBuffer.wrap( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Byte]].map( _.toDouble ) )
  }
  def toArray(buffer: DoubleBuffer): Array[Double] = if( buffer.hasArray ) buffer.array else { val data =for( index: Int <- (0 until buffer.capacity) ) yield buffer.get( index ); data.toArray }

  def toDoubleBuffer( buffer: Buffer ): DoubleBuffer = buffer match {
    case x: DoubleBuffer  => buffer.asInstanceOf[ DoubleBuffer ]
    case x => throw new Exception( "Attempt to convert non-float buffer to DoubleBuffer")
  }

  def factory( array: ucar.ma2.Array, invalid: Double = Double.MaxValue ): CDDoubleArray = {
    val storage = CDDoubleArray.toDoubleBuffer( array )
    new CDDoubleArray(new CDIndexMap(array.getShape), storage, invalid )
  }
}

class CDDoubleArray( cdIndexMap: CDIndexMap, val doubleStorage: DoubleBuffer, protected val invalid: Double ) extends CDArray[Double](cdIndexMap,doubleStorage) {

  protected def getData: DoubleBuffer = doubleStorage
  def getInvalid = invalid
  def getStorageValue( index: StorageIndex ): Double = doubleStorage.get( index )
  def setStorageValue( index: StorageIndex, value: Double ): Unit = doubleStorage.put( index, value )

  def this( shape: Array[Int], storage: DoubleBuffer, invalid: Double ) = this( CDIndexMap.factory(shape), storage, invalid )

  def valid( value: Double ) = ( value != invalid )

  override def dup(): CDDoubleArray = new CDDoubleArray( cdIndexMap.getShape, this.getSectionData().asInstanceOf[DoubleBuffer] , getInvalid )
  def spawn( shape: Array[Int], fillval: Double ): CDArray[Double] = CDArray.factory( shape, DoubleBuffer.wrap(Array.fill[Double]( shape.product )(fillval)), getInvalid )

  def copySectionData(maxSize: Int = Int.MaxValue): DoubleBuffer = {
    val array_data_iter = for ( index <- getIterator; if( index<maxSize); value = getStorageValue(index) ) yield value
    DoubleBuffer.wrap(array_data_iter.toArray)
  }

  def zeros: CDDoubleArray = new CDDoubleArray( getShape, DoubleBuffer.wrap(Array.fill[Double]( getSize )(0)), invalid )
  def invalids: CDDoubleArray = new CDDoubleArray( getShape, DoubleBuffer.wrap(Array.fill[Double]( getSize )(invalid)), invalid )

  override def getSectionData(maxSize: Int = Int.MaxValue): DoubleBuffer = super.getSectionData(maxSize).asInstanceOf[DoubleBuffer]
  def getStorageData: DoubleBuffer = doubleStorage
  def getStorageArray: Array[Double] = CDDoubleArray.toArray( doubleStorage )
  def getSectionArray(maxSize: Int = Int.MaxValue): Array[Double] = CDDoubleArray.toArray( getSectionData(maxSize) )
  def getArrayData(maxSize: Int = Int.MaxValue): Array[Double]  = if( isStorageCongruent ) getStorageArray else getSectionArray(maxSize)
}

//
//object ArrayTest extends App {
//  val base_shape = Array(5,5,5)
//  val subset_origin = Array(1,1,1)
//  val subset_shape = Array(2,2,2)
//  val storage = Array.iterate( 0f, 125 )( x => x + 1f )
//  val cdIndexMap: CDIndexMap = CDIndexMap.factory( base_shape )
//  val cd_array = CDArray.factory( cdIndexMap, storage, Float.MaxValue )
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
