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

object ParseHelp {
  def isInt( value: String ): Boolean = try { value.toInt; true } catch { case t: Throwable => false }
}

abstract class CommandHandler( val name: String, val description: String ) {
  val id: String = extractMatchId
  def process( state: ShellState ): ShellState
  def getPrompt( state: ShellState ): String
  def matches( command: String ): Boolean = command.toLowerCase.startsWith(id)
  def extractMatchId = name.toLowerCase.split('[').last.split(']').head
  def help: String = { s" * '$name': $description"}
}

class ShellState( val handlerStack: Vector[CommandHandler], val history: Array[String]= Array.empty[String]  ) {
  def pushHandler( handler: CommandHandler ): ShellState = { new ShellState( handlerStack :+ handler, history ) }
  def updateHandler( handler: CommandHandler ): ShellState = { new ShellState( handlerStack.dropRight(1) :+ handler, history ) }
  def popHandler( preserveBase: Boolean = true ): ShellState = { new ShellState( if( (handlerStack.length > 1) || !preserveBase ) handlerStack.dropRight(1) else handlerStack, history ) }
  def handleCommand( command: String ): ShellState = handlerStack.last.process( new ShellState( handlerStack, history :+ command ) )   // handlerStack.dropRight(1)
  def getPrompt = handlerStack.last.getPrompt( this )
  def getTopCommand = history.last
  def getStackStr = handlerStack.map( _.id ).mkString( "( ",", "," )")
}

class CommandShell( val baseHandler: CommandHandler) {
  protected val console = System.console()

  @tailrec
  private def execute( state: ShellState ): Unit = {
    val command: String = console.readLine( state.getPrompt )
    if( !quitRequested(command) ) execute( state.handleCommand( command ) )
  }

  protected def quitRequested( command: String ): Boolean = command.toLowerCase().startsWith("quit")
  def run = execute( new ShellState( Vector(baseHandler) ) )
}

final class MultiStepCommandHandler( name: String, description: String, val prompts: Array[String], val validators: Array[(String)=>Option[String]], val executor: (Array[String]) => Unit, val inputs: Array[String] = Array.empty[String]  )
  extends CommandHandler(name,description) {

  def process( state: ShellState ): ShellState = {
    val command = state.getTopCommand
    validators.head( command ) match {
      case None =>
        if ( prompts.length > 1 ) {
          state.updateHandler( new MultiStepCommandHandler(name, description, prompts.tail, validators.tail, executor, inputs :+ command ) )
        } else {
          executor( inputs :+ command )
          state.popHandler()
        }
      case Some( errorMsg ) =>
        val new_prompts = s"Input error: $errorMsg, please try again: " +: prompts.drop(1)
        state.updateHandler( new MultiStepCommandHandler(name, description, new_prompts, validators, executor, inputs ) )
    }
  }
  def getPrompt( state: ShellState ) = prompts.head

  private def valid( command: String, validator: String ): Boolean = {
    true
  }
}

final class ListSelectionCommandHandler( name: String, description: String, val getChoices: () => Array[String], val executor: (String) => Unit, var errorState: Boolean) extends CommandHandler(name,description) {
  def this( name: String, description: String, choices: Array[String], executor: (String) => Unit, errorState: Boolean = false ) = this( name, description, () => choices, executor, errorState )
  val choices: Array[String] = getChoices()
  val selectionList: String = choices.zipWithIndex.map { case (v, i) => s"\t $i: $v" } mkString ("\n")

  def process( state: ShellState): ShellState = {
    val command = state.getTopCommand
    if (command.isEmpty) { state.popHandler() }
    else try {
      executor(choices(command.toInt));
      state.popHandler()
    } catch {
      case t: Throwable => state.updateHandler( new ListSelectionCommandHandler(name, description, choices, executor, true) )
    }
  }

  def getPrompt( state: ShellState ) = if (errorState) "   Invalid entry, please try again: " else s"Options:\n$selectionList\n > Enter index of choice: "
}

class SelectionCommandHandler( name: String, description: String, val prompt: String, val handlers: Array[CommandHandler] ) extends CommandHandler(name,description) {
  def getPrompt( state: ShellState ) = prompt
  def process( state: ShellState ):  ShellState = {
    handlers.find( _.matches( state.getTopCommand ) ) match { case Some(handler) => state.pushHandler( handler ); case None => state.popHandler() }
  }
  override def help: String = { s"Commands: \n" + handlers.map( _.help ).mkString("\n") }
}

class EchoHandler(name: String, description: String, val prompt: String ) extends CommandHandler(name,description)  {
  def process( state: ShellState ): ShellState = { println( "Executing: " + state.getTopCommand  ); state.popHandler() }
  def getPrompt( state: ShellState ): String = prompt
}

class HelpHandler(name: String, description: String ) extends CommandHandler(name,description)  {
  def process( state: ShellState ): ShellState = { state.popHandler() }
  def getPrompt( state: ShellState ): String = state.handlerStack.head.help + "\n"
}

class HistoryHandler( name: String, val executor: (String) => Unit, var errorState: Boolean = false ) extends CommandHandler( name, "Displays and (optionally) executes commands from the shell history" )  {
  def process( state: ShellState): ShellState = {
    val choices: Array[String] = state.history.zipWithIndex.map { case (v, i) => s"\t $i: $v" }
    val command = state.getTopCommand
    if (command.isEmpty) { state.popHandler() }
    else try {
      executor(choices(command.toInt));
      state.popHandler()
    } catch {
      case t: Throwable => state.updateHandler( new HistoryHandler(name, executor, true) )
    }
  }
  def getPrompt( state: ShellState ): String = {
    val selectionList: String = state.history.zipWithIndex.map { case (v, i) => s"\t $i: $v" } mkString ("\n")
    if (errorState) "   Invalid entry, please try again: " else s"Command History:\n$selectionList\n > Enter index to execute: "
  }
}

object consoleTest extends App {
  val testHandlers = Array(
    new MultiStepCommandHandler( "[m]ultistep", "MultiStepCommandHandler", Array("Enter one >> ", "Enter two >> ", "Enter three >> "), Array( (x)=>None, (x)=>None, (x)=>None ), (vals) => println( vals.mkString(",") ) ),
    new ListSelectionCommandHandler( "[s]election", "ListSelectionCommandHandler", Array("value1", "value2", "value3"),  (value: String) => println( s"Selection: $value" )  ),
    new EchoHandler( "[e]cho", "EchoHandler", "Input Command >> " ),
    new HistoryHandler( "[hi]story",  (value: String) => println( s"History Selection: $value" )  ),
    new HelpHandler( "[h]elp", "Command Help" )
  )
  val shell = new CommandShell( new SelectionCommandHandler( "base", "BaseHandler", ">> ", testHandlers ) )
  shell.run
}







//  def process(command: String ): ExecuteResponse = {
//    if( command.startsWith("t") ) {
//      ExecuteResponse
//    }
//  }
//}


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
