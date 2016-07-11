package nasa.nccs.console
import java.io.{Console, PrintWriter, StringWriter}

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
  def validate( command: String, state: ShellState ): Option[String] = None
  def extractMatchId = name.toLowerCase.split('[').last.split(']').head
  def help: String = { s" * '$name': $description"}
  def getArgs( command: String ) = command.trim.replace(","," ").split("\\s+")
  def assertBounds( ival: Int, b0: Int, b1: Int ): Unit = if( (ival<b0) || (ival>b1) ) throw new Exception( "Index out of bounds" )
  override def toString = s"{$id}"
}

class ShellState( val handlerStack: Vector[CommandHandler], val history: Vector[String]= Vector.empty[String], val props: Map[String,Array[String]]=Map.empty[String,Array[String]] ) {
  def pushHandler( handler: CommandHandler ): ShellState = {
//    println( " push <<<< topHandler: %s".format( handlerStack.map(_.toString).mkString("{ ",", "," }") ) )
    val handlers: Vector[CommandHandler] = handlerStack :+ handler
//    println( " push >>>> topHandler: %s".format( handlers.map(_.toString).mkString("{ ",", "," }") ) )
    new ShellState( handlers, history, props )
  }
  def updateHandler( handler: CommandHandler ): ShellState = {
//    println( " update <<<< topHandler: %s".format( handlerStack.map(_.toString).mkString("{ ",", "," }") ) )
    val handlers: Vector[CommandHandler] = handlerStack.dropRight(1) :+ handler
//    println( " update >>>> topHandler: %s".format( handlers.map(_.toString).mkString("{ ",", "," }") ) )
    new ShellState( handlers, history, props )
  }
  def popHandler( preserveBase: Boolean = true ): ShellState = {
//    println( " pop <<<< topHandler: %s".format( handlerStack.map(_.toString).mkString("{ ",", "," }") ) )
    val handlers: Vector[CommandHandler] = if( (handlerStack.length > 1) || !preserveBase ) handlerStack.dropRight(1) else handlerStack
//    println( " pop>>>> topHandler: %s".format( handlers.map(_.toString).mkString("{ ",", "," }") ) )
    new ShellState( handlers, history, props )
  }
  def handleCommand( command: String ): ShellState = { handlerStack.last.process( new ShellState( handlerStack, history :+ command, props ) )   }
  def delegate( handler: CommandHandler ): ShellState = {  handler.process( pushHandler(handler) )  }
  def getPrompt = { handlerStack.last.getPrompt( this ) }
  def getTopCommand = history.last
  def getTopHandler = handlerStack.last
  def getStackStr = handlerStack.map( _.id ).mkString( "( ",", "," )")
  def sameHandler( handler: ShellState ): Boolean = { getTopHandler == handler.getTopHandler }
  def getProp( name: String ): Option[Array[String]] = props.get( name )
  def :+ ( new_props: Map[String,Array[String]] ): ShellState = new ShellState( handlerStack, history, props ++ new_props )
}

class CommandShell( val baseHandler: CommandHandler) {
  protected val console = System.console()

  @tailrec
  private def execute( state: ShellState ): Unit = {
    assert( console != null, "Can't get a console on this system!" )
    val command: String = console.readLine( state.getPrompt )
    if( !quitRequested(command) ) execute( state.handleCommand( command ) )
  }

  protected def quitRequested( command: String ): Boolean = command.toLowerCase().startsWith("quit")
  def run = execute( new ShellState( Vector(baseHandler) ) )
}

final class MultiStepCommandHandler( name: String, description: String, val prompts: Vector[String], val validators: Vector[(String)=>Option[String]], val executor: (Vector[String],ShellState) => ShellState, val length: Int = -1  )
  extends CommandHandler(name,description) {
  val _length = if( length > 0 ) length else prompts.length

  def process( state: ShellState ): ShellState = {
    val command = state.getTopCommand
    validators.head( command ) match {
      case None =>
        if ( prompts.length > 1 ) state.updateHandler( new MultiStepCommandHandler(name, description, prompts.tail, validators.tail, executor, _length ) )
        else                      executor( state.history.takeRight(_length), state ).popHandler()
      case Some( errorMsg ) =>
        val new_prompts = s"Input error: $errorMsg, please try again: " +: prompts.drop(1)
        state.updateHandler( new MultiStepCommandHandler(name, description, new_prompts, validators, executor, _length ) )
    }
  }
  def getPrompt( state: ShellState ) = prompts.head
}

final class SequentialCommandHandler( name: String, description: String, val handlers: Vector[CommandHandler], val executor: (ShellState) => ShellState, val errMsg: String = ""  )
  extends CommandHandler(name,description) {

  def process(state: ShellState): ShellState = {
    handlers.head.validate(state.history.last, state) match {
      case None =>
        val lastCommand = ( handlers.length == 1 )
        val updatedState = if(!lastCommand) { state.updateHandler(new SequentialCommandHandler(name, description, handlers.tail, executor)) } else state
        val processedState = updatedState.delegate(handlers.head)
        if( lastCommand ) { executor(processedState).popHandler() } else { processedState }
      case Some(errMsg) => state.updateHandler( new SequentialCommandHandler(name, description, handlers, executor, errMsg) )

    }
  }
  def getPrompt( state: ShellState ) = errMsg + handlers.head.getPrompt( state )
  override def toString = s"{$id:%s}".format( if(handlers.isEmpty) "" else handlers.head.id )
}

final class ListSelectionCommandHandler( name: String, description: String, val getChoices:(ShellState) => Array[String], val executor: (Array[String],ShellState) => ShellState, var errorState: Boolean = false) extends CommandHandler(name,description) {
  def getSelectionList(state: ShellState): String = getChoices(state).zipWithIndex.map { case (v, i) => s"\t $i: $v" } mkString ("\n")

  def process( state: ShellState): ShellState = {
    val command = state.getTopCommand
    if (command.isEmpty) { state.popHandler() }
    else try {
      val args = getArgs(command)
      // printf( "Processing args: " + args.mkString(",") )
      executor( args.map( arg => getChoices(state)(arg.toInt) ), state ).popHandler()
    } catch {
      case t: Throwable =>
        val sw = new StringWriter
        t.printStackTrace(new PrintWriter(sw))
        printf( sw.toString )
        state.updateHandler( new ListSelectionCommandHandler(name, description, getChoices, executor, true) )
    }
  }

  override def validate( command: String, state: ShellState ): Option[String] = {
    if( command.isEmpty ) None
    else try{ getArgs(command).foreach( sval => assertBounds( sval.toInt, 0, getChoices(state).length-1 )  ); None } catch { case ex: Throwable => Some("Entry error: %s\n".format(ex.getMessage)) }
  }

  def getPrompt( state: ShellState ) = if (errorState) "   Invalid entry, please try again: " else s"$description:\n%s\n > Enter index(es) of choice(s): ".format( getSelectionList(state: ShellState) )
}

class SelectionCommandHandler( name: String, description: String, val prompt: String, val cmd_handlers: Array[CommandHandler] ) extends CommandHandler(name,description) {
  val handlers = cmd_handlers.sortWith( (c0,c1) => c0.id.length > c1.id.length )
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

final class ExecutionHandler(name: String, description: String, val prompt: String, val executor: (ShellState) => Option[String], val validator: (String) => Option[String] = (String)=>None ) extends CommandHandler(name,description)  {
  def process( state: ShellState ): ShellState = executor( state  ) match {
    case None => state.popHandler();
    case Some( errorMsg ) =>
      val new_prompt = s"Input error: $errorMsg, please try again: "
      state.updateHandler( new ExecutionHandler(name, description, new_prompt, executor, validator ) )
  }
  def getPrompt( state: ShellState ): String = prompt
  override def validate( command: String, state: ShellState ) = validator( command )
}

object EntryHandler {
  def apply(name: String, prompt: String, validator: (String) => Option[String] = (String)=>None, description: String ="" ) = new EntryHandler( name, description, prompt, validator)
}
final class EntryHandler(name: String, description: String, val prompt: String, val validator: (String) => Option[String] = (String)=>None ) extends CommandHandler(name,description)  {
  def process( state: ShellState ): ShellState = state.popHandler()
  def getPrompt( state: ShellState ): String = prompt
  override def validate( command: String, state: ShellState ) = validator( command )
}

class HelpHandler(name: String, description: String ) extends CommandHandler(name,description)  {
  def process( state: ShellState ): ShellState = { state.popHandler() }
  def getPrompt( state: ShellState ): String = state.handlerStack.head.help + "\n"
}

final class HistoryHandler( name: String, val executor: (String) => Unit, var errorState: Boolean = false ) extends CommandHandler( name, "Displays and (optionally) executes commands from the shell history" )  {
  def process( state: ShellState): ShellState = {
    val choices: Vector[String] = state.history.zipWithIndex.map { case (v, i) => s"\t $i: $v" }
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
    new MultiStepCommandHandler( "[m]ultistep", "MultiStepCommandHandler", Vector("Enter one >> ", "Enter two >> ", "Enter three >> "), Vector( (x)=>None, (x)=>None, (x)=>None ), (vals,state) => { println( vals.mkString(",") ); state } ),
    new ListSelectionCommandHandler( "[s]election", "ListSelectionCommandHandler", (state) => Array("value1", "value2", "value3"),  (values: Array[String],state) => { println( "Selection: " + values.mkString(",") ); state }  ),
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
