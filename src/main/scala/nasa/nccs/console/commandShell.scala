package nasa.nccs.console
import java.io.Console

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.cds2.engine.MetadataPrinter
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.process.TaskRequest

import collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

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

object CommandExecutables {
  private val domainMap = new ConcurrentLinkedHashMap.Builder[String, Map[String,String]].initialCapacity(100).maximumWeightedCapacity(10000).build()
  private var currentDomain: String = "d0"
  domainMap.put( currentDomain, Map.empty[String,String] )

  def getDomain( domId: String = currentDomain ): Option[Map[String,String]] = Option(domainMap.get(domId))
  def putDomain( domId: String, domain: Map[String,String] ) = domainMap.put( domId, domain )

  private val values: List[CommandExecutable] = List(

    new CommandExecutable("[t]est", "Test exe", "") {
      def execute(command: String, callIndex: Int ): Boolean = {
        println("---> Executing command: " + command + ", call index = " + callIndex );
        false
      }
    },
    new CommandExecutable("[he]lp", "Lists available commands", "") {
      def execute(command: String, callIndex: Int ): Boolean = {
        println( "------ Commands --------" )
        for( cmdExe <- CommandExecutables.getCommandsAlpha ) {
          println( "  --> %s %s: %s ".format( cmdExe.name, cmdExe.args, cmdExe.description) )
        }
        false
      }
    },
    new CommandExecutable("[ca]che", "Cache variable from NetCDF dataset", "<collection_id> <variable> <domain> <dataset_path>") {
      def execute(command: String, callIndex: Int ): Boolean = {
        println( "------ Commands --------" )
        for( cmdExe <- CommandExecutables.getCommandsAlpha ) {
          println( "  --> %s %s: %s ".format( cmdExe.name, cmdExe.args, cmdExe.description) )
        }
        false
      }
    },
    new CommandExecutable("[co]llections", "Collection Operations: [l]ist, [d]efine, [s]electCurrent", "<operation:(l/d/s)>") {
      def execute(command: String, callIndex: Int ): Boolean = {
        interactionHandler match {
          case None =>
            val cmdArgs = command.split(' ')
            val operation = if (cmdArgs.length > 1) {
              cmdArgs(1)
            } else {
              "list"
            }.toLowerCase.head
            operation match {
              case 'l' =>
                println("------ Collections --------")
                for (collId <- Collections.idSet) Collections.findCollection(collId) match {
                  case Some(collection) => println("  --> id: %s vars: (%s), url: %s, path: %s ".format(collId, collection.url, collection.vars.mkString(","), collection.path))
                  case None => Unit
                }
                false
              case 'd' =>
                interactionHandler = Some( new CommandInterationHandler(
                  List( "Collection id:", "Dataset url or file path:" ), ( responses: List[String] ) => Collections.addCollection( responses(0), responses(1) ) ) )
                true
              case 's' =>
                interactionHandler = Some( new CommandInterationHandler(
                  List( Collections.indexedCollectionList ), ( responses: List[String] ) => Collections.addCollection( responses(0), responses(1) ) ) )
                true
              case x =>
                println("Unrecognized <operation> argument: " + operation)
                false
            }
          case Some( handler ) =>
            handler.process( command )
        }
      }
    }
  )
  val getCommands: List[CommandExecutable] = values.sortWith(_.len > _.len)
  val getCommandsAlpha: List[CommandExecutable] = values.sortWith( _.key < _.key )
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

object commandShell {
  private val console = System.console()

  def run = Iterator.continually( console.readLine( commandProcessor.getCursor ) ).takeWhile(!_.startsWith("quit")).foreach( command => commandProcessor.process( command ) )

}

object consoleTest extends App {
  commandShell.run
}




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
