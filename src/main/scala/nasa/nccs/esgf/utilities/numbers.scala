package nasa.nccs.esgf.utilities.numbers
import nasa.nccs.utilities.Loggable

import scala.language.implicitConversions

class IllegalNumberException( value: Any ) extends RuntimeException("Error, " + value.toString + " is not a valid Number")

object GenericNumber {
  implicit def int2gen( numvalue: Int ): GenericNumber = { new IntNumber(numvalue) }
  implicit def float2gen( numvalue: Float ): GenericNumber = { new FloatNumber(numvalue) }
  implicit def long2gen( numvalue: Long ): GenericNumber = { new LongNumber(numvalue) }
  implicit def dbl2gen( numvalue: Double ): GenericNumber = { new DoubleNumber(numvalue) }
  implicit def short2gen( numvalue: Short ): GenericNumber = { new ShortNumber(numvalue) }
  implicit def gen2int( gennum: GenericNumber ): Int = { gennum.toInt }
  implicit def gen2float( gennum: GenericNumber ): Float = { gennum.toFloat }
  implicit def gen2long( gennum: GenericNumber ): Long = { gennum.toLong }
  implicit def gen2double( gennum: GenericNumber ): Double = { gennum.toDouble }
  implicit def gen2string( gennum: GenericNumber ): String = { gennum.toString }
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"")

  def parse(sx: String): GenericNumber = {
    try {
      new IntNumber(sx.toInt)
    } catch {
      case err: NumberFormatException => try {
        new FloatNumber(sx.toFloat)
      } catch {
        case err: NumberFormatException => try {
          new LongNumber(sx.toLong)
        } catch {
          case err: NumberFormatException => throw new IllegalNumberException(sx)
        }
      }
    }
  }
  def apply( anum: Any = None ): GenericNumber = {
    anum match {
      case ix: Int =>     new IntNumber(ix)
      case fx: Float =>   new FloatNumber(fx)
      case lx: Long =>    new LongNumber(lx)
      case dx: Double =>  new DoubleNumber(dx)
      case sx: Short =>   new ShortNumber(sx)
      case stx: String => new StringNumber(stx)
      case None =>        new UndefinedNumber()
      case x =>           throw new IllegalNumberException(x)
    }
  }
}

abstract class GenericNumber extends Loggable {
  type NumericType
  def value: NumericType
  override def toString = value.toString
  def toInt: Int
  def toLong: Long
  def toFloat: Float
  def toDouble: Double
}

class StringNumber( val numvalue: String ) extends GenericNumber {
  type NumericType = String
  override def value: NumericType = GenericNumber.normalize(numvalue)
  override def toInt: Int = value.toInt
  override def toLong: Long = value.toLong
  override def toFloat: Float = { value.toFloat }
  override def toDouble: Double = { value.toDouble }
}

class IntNumber( val numvalue: Int ) extends GenericNumber {
  type NumericType = Int
  override def value: NumericType = numvalue
  override def toInt: Int = value
  override def toLong: Long = value.toLong
  override def toFloat: Float = { value.toFloat }
  override def toDouble: Double = { value.toDouble }
}

class FloatNumber( val numvalue: Float ) extends GenericNumber {
  type NumericType = Float
  override def value: NumericType =  numvalue
  override def toInt: Int = { value.toInt }
  override def toFloat: Float = value
  override def toLong: Long = value.toLong
  override def toDouble: Double = value.toDouble
}

class DoubleNumber( val numvalue: Double ) extends GenericNumber {
  type NumericType = Double
  override def value: NumericType = numvalue
  override def toInt: Int = { value.toInt }
  override def toFloat: Float = value.toFloat
  override def toLong: Long = value.toLong
  override def toDouble: Double = value
}

class ShortNumber( val numvalue: Short ) extends GenericNumber {
  type NumericType = Short
  override def value: NumericType = numvalue
  override def toInt: Int = value.toInt
  override def toLong: Long = value.toLong
  override def toFloat: Float = { value.toFloat }
  override def toDouble: Double = { value.toDouble }
}

class LongNumber( val numvalue: Long ) extends GenericNumber {
  type NumericType = Long
  override def value: NumericType = numvalue
  override def toInt: Int = value.toInt
  override def toLong: Long = value
  override def toFloat: Float = { value.toFloat }
  override def toDouble: Double = { value.toDouble }
}


class UndefinedNumber extends GenericNumber {
  type NumericType = Option[Any]
  override def value: NumericType = None
  override def toInt: Int = throw new Exception( "Attempt to access an UndefinedNumber ")
  override def toFloat: Float = throw new Exception( "Attempt to access an UndefinedNumber ")
  override def toDouble: Double = throw new Exception( "Attempt to access an UndefinedNumber ")
  override def toLong: Long = throw new Exception( "Attempt to access an UndefinedNumber ")
}



