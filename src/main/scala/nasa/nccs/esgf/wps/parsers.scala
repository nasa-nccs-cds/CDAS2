package nasa.nccs.esgf.wps
import nasa.nccs.utilities.Loggable

import scala.util.parsing.combinator._

class BadRequestException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

class ObjectNotationParser extends JavaTokenParsers {
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase
  def expr: Parser[Map[String, Seq[Map[String, Any]]]] = "[" ~> repsep(decl,sep) <~ "]" ^^ (Map() ++ _)
  def decl: Parser[(String, Seq[Map[String, Any]])] = key ~ "=" ~ objlist ^^ { case arg0 ~ "=" ~ arg1 => (normalize(arg0), arg1) }
  def key: Parser[String] = """[a-zA-Z_]\w*""".r
  def sep: Parser[String] = """[,;]""".r
  def integerNumber: Parser[String] = """[+-]?(?<!\.)\b[0-9]+\b(?!\.[0-9])""".r
  def value: Parser[Any] = (
    stringLiteral ^^ (_.stripPrefix(""""""").stripSuffix("""""""))
      | omap
      | slist ^^ ( _.map( _.stripPrefix(""""""").stripSuffix(""""""")) )
      | integerNumber ^^ (_.toInt)
      | floatingPointNumber ^^ (_.toFloat)
      | "true" ^^ (x => true)
      | "false" ^^ (x => false)
    )
  def member:  Parser[(String, Any)] = stringLiteral ~ ":" ~ value ^^ { case x ~ ":" ~ y => (normalize(x), y) }
  def omap: Parser[Map[String, Any]] = "{" ~> repsep(member,sep) <~ "}" ^^ (Map() ++ _)
  def slist: Parser[List[String]] = "[" ~> repsep( stringLiteral | integerNumber | floatingPointNumber, sep ) <~ "]" ^^ (List[String]() ++ _ )
  def objlist: Parser[Seq[Map[String, Any]]] = "[" ~> repsep(omap,sep) <~ "]" | omap ^^ (List(_))
}

object CDSecurity {
  def sanitize( str_data: String ): String = {
    if (str_data contains "]]>") throw new SecurityException(" Request contains illegal CDATA breakout string")
    str_data
  }
}

object wpsObjectParser extends ObjectNotationParser with Loggable {

  def cdata(obj: Any): String = "<![CDATA[\n " + obj.toString + "\n]]>"

  def parseMap(args_input: String): Map[String, Any] = {
    try {
      CDSecurity.sanitize( args_input )
      parseAll(omap, args_input) match {
        case result: Success[_] => result.get.asInstanceOf[Map[String, Any]]
        case err: Error =>
          logger.error("Error Parsing '%s'".format(args_input) )
          throw new BadRequestException(err.toString)
        case err: Failure =>
          logger.error("Error Parsing '%s'".format(args_input) )
          throw new BadRequestException(err.toString)
      }
    } catch {
      case e: Exception =>
        logger.error("Error[%s] Parsing '%s': %s".format( e.getClass.getName, args_input, e.getMessage ) )
        throw new BadRequestException(e.getMessage, e)
    }
  }

  def parseDataInputs(data_input: String): Map[String, Seq[Map[String, Any]]] = {
    try {
      CDSecurity.sanitize( data_input )
      parseAll(expr, data_input) match {
        case result: Success[_] => result.get.asInstanceOf[Map[String, Seq[Map[String, Any]]]]
        case err: Error =>
          logger.error("Error Parsing '%s'".format(data_input) )
          throw new BadRequestException(err.toString)
        case err: Failure =>
          logger.error("Error Parsing '%s'".format(data_input) )
          throw new BadRequestException(err.toString)
      }
    } catch {
      case e: Exception =>
        logger.error("Error[%s] Parsing '%s': %s".format( e.getClass.getName, data_input, e.getMessage ) )
        throw new BadRequestException(e.getMessage, e)
    }
  }
}

