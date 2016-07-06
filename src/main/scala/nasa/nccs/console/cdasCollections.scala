package nasa.nccs.console
import java.nio.file.{Files, Paths}
import nasa.nccs.cdapi.cdm.Collection
import nasa.nccs.cds2.loaders.Collections

import nasa.nccs.cdapi.kernels.ExecutionResults
import nasa.nccs.cds2.engine.CDS2ExecutionManager
import nasa.nccs.esgf.process.TaskRequest

class CdasCollections( executionManager: CDS2ExecutionManager ) {
  val printer = new xml.PrettyPrinter(200, 3)

  def generateAggregation(inputs: Vector[String], state: ShellState ): ShellState = {
    if( inputs.forall( !_.isEmpty ) ) {
      val uri: String = "collection:/" + inputs(0).trim.toLowerCase
      val collection = Collections.addCollection( uri, inputs(1).trim() )
      collection.createNCML()
    }
    state
  }

  def cacheVariables( state: ShellState ): ShellState = {
    state.getProp("variables")  match {
      case Some(varRecs) =>
        val results: Array[ExecutionResults] = varRecs.map( (varRec) => {
          val vtoks = varRec.split(':').map(_.trim)
          val varname = vtoks(1)
          val uri: String = "collection:/" + vtoks(0)
          val dataInputs = Map("variable" -> List(Map("uri" -> uri, "name" -> varname, "domain" -> "d0")))
          executeTask(TaskRequest("util.cache", dataInputs))
        })
        state :+ Map( "results" -> Array.empty[String] )
      case None =>
        println( "No selected variables" )
        state
    }

//        variables.map(cid => Collections.findCollection(cid) match {
//
//        }
////    state.getProp("collections") match {
////      case Some( cids ) => cids.foreach( (cid) => {
////
////      }
////    }
//    state.getProp()
//    printf( " Cache Variables: " + inputs.mkString(",") )
////    val uri: String = "collection:/" + inputs(0)
////    val varnames = inputs(1).toLowerCase.trim.replace(","," ").split("\\s+")
////    val results: Array[ExecutionResults] = for( varname <- varnames ) yield {
////      val dataInputs = Map("variable" -> List(Map("uri" -> uri, "name" -> varname, "domain" -> inputs(2))))
////      executeTask( TaskRequest("util.cache", dataInputs) )
//    }
    state
  }

  def validCollectionId( exists: Boolean )( id: String ): Option[String] = {
    if(Collections.getCollectionKeys.contains(id)) {
      if(exists) None else Some( s"collection $id exists")
    } else {
      if(exists) Some( s"collection $id does not exist" ) else None
    }
  }
  def validDirecory( dir: String ): Option[String] = { if( Files.exists(Paths.get(dir.trim))) None else Some( s"Directory '$dir' does not exist" ) }
  def validDomainId( domId: String ): Option[String] = { None }
  def validVariables( vars: String ): Option[String] = { None }

  def getAggregateCommand: MultiStepCommandHandler = {
    new MultiStepCommandHandler("[ag]gregate", "Create collection by defining aggregated dataset",
      Vector( "Enter collection id >>", "Enter path to dataset directory >>" ),
      Vector( validCollectionId(false) _, validDirecory ),
      generateAggregation
    )
  }

  def getCacheCommand: SequentialCommandHandler = {
    new SequentialCommandHandler("[ca]che", "Cache variable[s] from a collection",
      Vector( getSelectCollectionsCommand, getSelectVariablesCommand  ),
      cacheVariables
    )
  }

  def getVariableList(state: ShellState): Array[String] = {
    state.getProp("collections")  match {
      case Some( collections ) => {
        collections.map(cid => Collections.findCollection(cid) match {
          case Some(collection) => collection.vars.map(v => collection.id + ": " + v).toArray
          case None => Array.empty[String]
        }).foldLeft(Array.empty[String]) { _ ++ _ }
      }
      case None => println( "++++ UNDEF Collections! "); Array.empty[String]
    }
  }

  def getListCollectionsCommand: ListSelectionCommandHandler = {
    new ListSelectionCommandHandler("[lc]ollections", "List collection metadata", Collections.getCollectionKeys, (cids:Array[String],state) => { cids.foreach( cid => printCollectionMetadata( cid ) ); state } )
  }
  def getDeleteCollectionsCommand: ListSelectionCommandHandler = {
    new ListSelectionCommandHandler("[dc]ollections", "Delete specified collections", Collections.getCollectionKeys, (cids:Array[String],state) => { cids.foreach( cid => Collections.removeCollection( cid ) ); state } )
  }
  def getSelectCollectionsCommand: ListSelectionCommandHandler = {
    new ListSelectionCommandHandler("[sc]ollections", "Select collection(s)", Collections.getCollectionKeys, ( cids:Array[String], state ) => state :+ Map( "collections" -> cids )  )
  }
  def getSelectVariablesCommand: ListSelectionCommandHandler = {
    new ListSelectionCommandHandler("[sv]ariables", "Select variables from selected collection(s)", getVariableList, (cids:Array[String],state) => { state :+ Map( "variables" -> cids ) } )
  }

  def executeTask( taskRequest: TaskRequest, runArgs: Map[String,String] = Map.empty[String,String] ): ExecutionResults = executionManager.blockingExecute(taskRequest, runArgs)
  def printCollectionMetadata( collectionId: String  ): Unit = println( printer.format( Collections.getCollectionXml( collectionId ) ) )
}

object collectionsConsoleTest extends App {
  val cdasCollections = new CdasCollections( new CDS2ExecutionManager(Map.empty) )
  val handlers = Array(
    cdasCollections.getAggregateCommand,
    cdasCollections.getCacheCommand,
    cdasCollections.getListCollectionsCommand,
    cdasCollections.getDeleteCollectionsCommand,
    new HistoryHandler( "[hi]story",  (value: String) => println( s"History Selection: $value" )  ),
    new HelpHandler( "[h]elp", "Command Help" )
  )
  val shell = new CommandShell( new SelectionCommandHandler( "base", "BaseHandler", "cdas> ", handlers ) )
  shell.run
}

// nasa.nccs.console.collectionsConsoleTest

object collectionsTest extends App {
  val cid = "merra_1/hourly/aggtest"
  Collections.removeCollection( cid )
}