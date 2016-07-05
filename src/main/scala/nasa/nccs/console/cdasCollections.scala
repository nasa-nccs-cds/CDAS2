package nasa.nccs.console
import java.nio.file.{Files, Paths}
import nasa.nccs.cdapi.cdm.Collection
import nasa.nccs.cds2.loaders.Collections

import nasa.nccs.cdapi.kernels.ExecutionResults
import nasa.nccs.cds2.engine.CDS2ExecutionManager
import nasa.nccs.esgf.process.TaskRequest

class cdasCollections( executionManager: CDS2ExecutionManager ) {

  def generateAggregation(inputs: Vector[String]): Unit = {
    val uri: String = "collection:/" + inputs(0)
    Collections.addCollection( uri, inputs(1) )
  }

  def cacheVariables(inputs: Vector[String]): Unit = {
    val uri: String = "collection:/" + inputs(0)
    val varnames = inputs(1).toLowerCase.trim.replace(","," ").split("\\s+")
    val results: Array[ExecutionResults] = for( varname <- varnames ) yield {
      val dataInputs = Map("variable" -> List(Map("uri" -> uri, "name" -> varname, "domain" -> inputs(2))))
      executeTask( TaskRequest("util.cache", dataInputs) )
    }
  }

  def validCollectionId( exists: Boolean )( id: String ): Option[String] = {
    if(Collections.getCollectionKeys.contains(id)) {
      if(exists) None else Some( s"collection $id exists")
    } else {
      if(exists) Some( s"collection $id does not exist" ) else None
    }
  }
  def validDirecory( dir: String ): Option[String] = { if( Files.exists(Paths.get(dir))) None else Some( s"Directory '$dir' does not exist" ) }
  def validDomainId( domId: String ): Option[String] = { None }
  def validVariables( vars: String ): Option[String] = { None }

  def getAggregateCommand: MultiStepCommandHandler = {
    new MultiStepCommandHandler("[co]llection", "Create collection by defining aggregated dataset",
      Vector( "Enter collection id >>", "Enter path to dataset directory >>" ),
      Vector( validCollectionId(false) _, validDirecory ),
      generateAggregation
    )
  }

  def getCacheCommand: MultiStepCommandHandler = {
    new MultiStepCommandHandler("[ca]che", "Cache variable[s] from a collection",
      Vector("Enter collection id >>", "Enter variables to cache >>", "Enter domain id (default: d0) >>" ),
      Vector( validCollectionId(true) _, validVariables, validDomainId ),
      cacheVariables
    )
  }

  def executeTask( taskRequest: TaskRequest, runArgs: Map[String,String] = Map.empty[String,String] ): ExecutionResults = executionManager.blockingExecute(taskRequest, runArgs)

}
