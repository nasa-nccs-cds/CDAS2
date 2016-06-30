package nasa.nccs.console
import java.io.Console

import nasa.nccs.cds2.engine.MetadataPrinter

import collection.mutable

object ArrowType {
  sealed abstract class Value( val index: Byte ) { def compare( index1: Byte ): Boolean = { index == index1 } }
  case object Up extends Value(0x41)
  case object Down extends Value(0x42)
  case object Right extends Value(0x43)
  case object Left extends Value(0x44)
  case object None extends Value(0)
  val types = Seq( Up, Down, Right, Left )
}

abstract class CommandExecutable( val name: String, val description: String, val args: String ) {
  val key = name.split('[').last.split(']').head
  def matches( command: String ): Boolean = command.startsWith(key)
  def execute( command: String ): Boolean
}

object CommandExecutables {
  private val values = List(
    new CommandExecutable( "[t]est", "Test exe", "" ) {
      def execute( command: String ): Boolean = { println( "Executing command: " + command ); true }
    }
  )
}

object commandProcessor {
  private val cursor = ">> "
  private val history = new mutable.MutableList[String]

  def getCursor(): String = cursor

  def process(cmd: String) = {}
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
