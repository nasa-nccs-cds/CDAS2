package nasa.nccs.esgf.utilities

import scala.util.parsing.combinator._


class OperationNotationParser extends JavaTokenParsers {
  var key_index = 0
  def new_key: String = { key_index += 1; "ivar#" + key_index }
  def expr: Parser[Map[String, Any]] = repsep(function, ",") ^^ (Map() ++ _)
  def arglist: Parser[List[String]] = "(" ~> repsep(value, ",") <~ ")"
  def value: Parser[String] = """[a-zA-Z0-9_ :.*|]*""".r
  def name: Parser[String] = """[a-zA-Z0-9_.]*""".r
  def fname: Parser[String] = (
    name ~ ":" ~ name ^^ { case x ~ ":" ~ y => y + "~" + x }
      | name ^^ (y => y + "~" + new_key)
    )
  def function: Parser[(String, List[String])] = (
    fname ~ arglist ^^ { case x ~ y => (x, y) }
      | arglist ^^ { y => (new_key, y) }
    )
}

object wpsOperationParser extends OperationNotationParser {
  def parseOp(operation: String): Map[String, Any] = parseAll(expr, operation.stripPrefix("\"").stripSuffix("\"")).get
}

object testOperationParser extends App {
  val input = "v3:CWT.average(v0,axis:xy)"
  val parsed_input = wpsOperationParser.parseOp( input )
  println( parsed_input.toString )
}

object wpsNameMatchers {
  val lonAxis = """^lon\w*""".r
  val latAxis = """^lat\w*""".r
  val levAxis = """^lev\w*|^plev\w*""".r
  val yAxis = """^y\w*""".r
  val xAxis = """^x\w*""".r
  val zAxis = """^z\w*""".r
  val tAxis = """^t\w*""".r

  def getDimension( axisName: String ): Char = axisName match {
    case xAxis() => 'x'
    case yAxis() => 'y'
    case zAxis() => 'z'
    case tAxis() => 't'
    case _ => throw new Exception( "Unrecognized axis name: " + axisName )
  }
}

object wpsNameMatchNest extends App {
  val axis_name = "xdim"
  val dimension = wpsNameMatchers.getDimension( axis_name )
  println( "Result = " + dimension )
}




