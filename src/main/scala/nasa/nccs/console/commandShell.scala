package nasa.nccs.console
import java.io.Console

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.cds2.engine.MetadataPrinter
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.process.TaskRequest

import collection.mutable
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future


trait CommandHandler {
  def process( command: String ): ExecuteResponse
}

object ExecuteResponse {
  def apply( prompt: String, handler:CommandHandler ) = { new ExecuteResponse( Some(prompt), Some(handler) ) }
  def empty = new ExecuteResponse( None, None)
}
class ExecuteResponse( val prompt: Option[String], val handler: Option[CommandHandler] ) {}

class CommandShell( val basePrompt: String, val baseHandler: CommandHandler) {
  protected val console = System.console()

  @tailrec
  private def execute( response: ExecuteResponse, history: Array[String]= Array.empty[String] ): Unit = {
    val command: String = console.readLine( getPrompt(response) )
    if( !quitRequested(command) ) execute( getHandler(response).process(command), history :+ command   )
  }

  protected def getPrompt( response: ExecuteResponse ): String = response.prompt match { case None => basePrompt; case Some( prompt ) => response.handler match { case None => prompt + "\n" + basePrompt; case Some( handler ) => prompt } }
  protected def getHandler( response: ExecuteResponse ): CommandHandler = response.handler match { case None => baseHandler; case Some( handler ) => handler }
  protected def quitRequested( command: String ): Boolean = command.toLowerCase().startsWith("quit")

  def run = execute( ExecuteResponse( basePrompt, baseHandler) )

}

class MultiStepCommandHandler( val prompts: Array[String], val validators: Array[String], val executor: (Array[String]) => Unit, val inputs: Array[String] = Array.empty[String]  ) extends CommandHandler {

  def process(command: String): ExecuteResponse = {
    if( valid( command, validators.head ) ) {
      val accum_inputs = inputs :+ command
      if ( prompts.length > 1 ) {
        ExecuteResponse( prompts.tail.head, new MultiStepCommandHandler(prompts.tail, validators.tail, executor, accum_inputs ))
      } else {
        executor( accum_inputs )
        ExecuteResponse.empty
      }
    } else {
      ExecuteResponse("Invalid response, please try again: ", this )
    }
  }
  def getPrompt = prompts.head

  private def valid( command: String, validator: String ): Boolean = {
    true
  }
}

object testEchoHandler extends CommandHandler {
  def process( command: String ): ExecuteResponse = new ExecuteResponse( Some("Executing command: " + command ), None )
}

object testBaseHandler extends CommandHandler {
  def process( command: String ): ExecuteResponse = {
    if(command.startsWith("c")) {
      val handler = new MultiStepCommandHandler( Array("Enter one >> ", "Enter two >> ", "Enter three >> "), Array("i1", "i2", "i3"), (vals) => println( vals.mkString(",") ) ) {}
      ExecuteResponse( handler.getPrompt, handler )
    }
    else new ExecuteResponse( Some("Executing command: " + command ), None )
  }
}

//  def process(command: String ): ExecuteResponse = {
//    if( command.startsWith("t") ) {
//      ExecuteResponse
//    }
//  }
//}

object consoleTest extends App {
  val shell = new CommandShell( ">>", testBaseHandler )
  shell.run
}

//class ListSelectionCommandHandler( choices: Array[String], executor: (String) => Unit ) extends CommandHandler {
//
//  def process(command: String): ExecuteResponse = {
//
//  }
//







/*


object ArrowType {
  sealed abstract class Value( val index: Byte ) { def compare( index1: Byte ): Boolean = { index == index1 } }
  case object Up extends Value(0x41)
  case object Down extends Value(0x42)
  case object Right extends Value(0x43)
  case object Left extends Value(0x44)
  case object None extends Value(0)
  val types = Seq( Up, Down, Right, Left )
}

class CommandInterationHandler( val prompts: Seq[String], val execute: ( List[String] ) => Unit ) {
  val responses = new scala.collection.mutable.Queue[String]
  def prompt( callIndex: Int ): String = prompts(callIndex)
  def process( resp: String ): Boolean = {
    responses += resp
    if( responses.length == prompts.length ) {
      execute( responses.toList )
      false
    } else { true }
  }
}

abstract class CommandExecutable( val name: String, val description: String, val args: String  ) {
  val key = name.split('[').last.split(']').head
  val len = key.length
  def matches( command: String ): Boolean = command.startsWith(key)
  def execute( command: String, callIndex: Int  ): Boolean
  var interactionHandler: Option[CommandInterationHandler] = None

  def getTaskRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggTest", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/", "name" -> "t", "domain" -> "d0")))
    TaskRequest("util.cache", dataInputs)
  }
}


object commandProcessor {
  private val cursor = ">> "
  private val history = new mutable.MutableList[String]
  lazy val commands: List[CommandExecutable] = CommandExecutables.getCommands
  var activeCommandExe: Option[CommandExecutable] = None
  var callIndex = 0

  def getCursor(): String = cursor

  def process(cmd: String) = activeCommandExe match {
    case Some( cmdExe ) =>
      execute( cmd, cmdExe )
    case None =>
      commands.find( _.matches(cmd) ) match {
        case Some( cmdExe ) =>  execute( cmd, cmdExe )
        case None =>            println( "Unrecognized command: " + cmd )
      }
  }

  def execute( cmd: String, cmdExe: CommandExecutable ) = {
    activeCommandExe = if( cmdExe.execute( cmd, callIndex ) ) { callIndex += 1; Some(cmdExe) } else { callIndex = 0; None }
  }
}

object commandShell1 {
  private val console = System.console()

  def run = Iterator.continually( console.readLine( commandProcessor.getCursor ) ).takeWhile(!_.startsWith("quit")).foreach( command => commandProcessor.process( command ) )

}

object consoleTest extends App {
  commandShell1.run
}

*/



/*
object commandProcessor {
  private val cursor = ">> "
  private val history = new mutable.MutableList[String]
  private var historyIndex = 0
  private var exeBuffer: String = ""

  def process( command: String ) = {
    getArrowType( command ) match {
      case ArrowType.None =>
        history += command
        processCommand( command )
      case ArrowType.Up =>
        historyIndex += 1
        val hcmd: String = history.get( history.length - historyIndex ) match {
          case None =>
            historyIndex = 0
            history.get( historyIndex ).getOrElse("")
          case Some( cmd ) => cmd
        }
        exeBuffer = hcmd
      case ArrowType.Down =>
        historyIndex -= 1
        val hcmd: String = history.get( history.length - historyIndex ) match {
          case None =>
            historyIndex = history.length -1
            history.get( historyIndex ).getOrElse("")
          case Some( cmd ) => cmd
        }
        exeBuffer = hcmd
      case ArrowType.Right => println("Right" )
      case ArrowType.Left => println("Left" )
    }
  }

  def processCommand( cmd: String ) = {
    val command = if( cmd.isEmpty ) { exeBuffer } else cmd
    exeBuffer = ""
    println("Processing Command: " + command )
    val tokens = command.split(' ')
    if( tokens(0).startsWith("d") ) {
      MetadataPrinter.display(1)
    }
  }

  def getCursor(): String = { cursor + exeBuffer }

  def getArrowType( command: String  ): ArrowType.Value = {
    if( (command.length == 3) && (command(0).toInt == 0x1b) && (command(1).toInt == 0x5b) ) {
      ArrowType.types.find( at => at.compare( command(2).toByte ) ) match {
        case Some( value ) => value
        case None => ArrowType.None
      }
    } else { ArrowType.None }
  }

}*/
