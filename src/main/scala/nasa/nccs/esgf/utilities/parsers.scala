package nasa.nccs.esgf.utilities

import scala.io.Source
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

object wpsNameMatchers {
  val yAxis = """^lat\w*""".r
  val xAxis = """^lon\w*""".r
  val zAxis = """^lev\w*|^plev\w*""".r
  val id    = """^id\w*|^name\w*""".r
  val tAxis = """^tim\w*""".r

  def getDimension( axisName: String ): Char = axisName match {
    case xAxis() => 'x'
    case yAxis() => 'y'
    case zAxis() => 'z'
    case tAxis() => 't'
    case _ => throw new Exception( "Unrecognized axis name: " + axisName )
  }
}

//object readTest extends App {
//  val filename = "/Users/tpmaxwel/.cdas/cache/ncdump.test"
//  var timeData = false
//  var elem_count: Int = 0
//  for (line <- Source.fromFile(filename).getLines) {
//    if( line.startsWith(" time = ") ) { timeData = true }
//    val elems: Int = if( timeData ) { line.count( _ equals ',' ) } else 0
//    elem_count = elem_count + elems
//  }
//  print( elem_count.toString )
//}
//



