package nasa.nccs.cdas.utilities
import nasa.nccs.utilities.Logger
import ucar.nc2

object runtime {
  def printMemoryUsage(logger: Logger) = {
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    logger.info("--------------------------------- MEMORY USAGE ---------------------------------")
    logger.info("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb) + " M"
    logger.info("** Free Memory:  " + runtime.freeMemory / mb) + " M"
    logger.info("** Total Memory: " + runtime.totalMemory / mb) + " M"
    logger.info("** Max Memory:   " + runtime.maxMemory / mb) + " M"
    logger.info("** Processors:   " + runtime.availableProcessors )
    logger.info("--------------------------------- ------------ ---------------------------------")
  }

  def printMemoryUsage = {
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("--------------------------------- MEMORY USAGE ---------------------------------")
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb) + " M"
    println("** Free Memory:  " + runtime.freeMemory / mb) + " M"
    println("** Total Memory: " + runtime.totalMemory / mb) + " M"
    println("** Max Memory:   " + runtime.maxMemory / mb) + " M"
    println("** Processors:   " + runtime.availableProcessors )
    println("--------------------------------- ------------ ---------------------------------")
  }
}







