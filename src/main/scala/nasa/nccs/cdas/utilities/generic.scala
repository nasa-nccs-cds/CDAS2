package nasa.nccs.cdas.utilities
import nasa.nccs.utilities.Logger
import ucar.nc2

object runtime {
  def printMemoryUsage(logger: Logger) = {
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    logger.info("--------------------------------- MEMORY USAGE ---------------------------------")
    logger.info("** Used Memory: %d M ".format((runtime.totalMemory - runtime.freeMemory) / mb))
    logger.info("** Free Memory: %d M ".format(runtime.freeMemory / mb))
    logger.info("** Total Memory: %d M ".format(runtime.totalMemory / mb))
    logger.info("** Max Memory: %d M ".format(runtime.maxMemory / mb))
    logger.info("** Processors:   " + runtime.availableProcessors )
    logger.info("--------------------------------- ------------ ---------------------------------")
  }

  def printMemoryUsage = {
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("--------------------------------- MEMORY USAGE ---------------------------------")
    println("** Used Memory: %d M ".format((runtime.totalMemory - runtime.freeMemory) / mb))
    println("** Free Memory: %d M ".format(runtime.freeMemory / mb))
    println("** Total Memory: %d M ".format(runtime.totalMemory / mb))
    println("** Max Memory: %d M ".format(runtime.maxMemory / mb))
    println("** Processors:   " + runtime.availableProcessors )
    println("--------------------------------- ------------ ---------------------------------")
  }
}







