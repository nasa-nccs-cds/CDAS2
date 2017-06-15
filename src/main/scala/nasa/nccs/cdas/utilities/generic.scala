package nasa.nccs.cdas.utilities
import nasa.nccs.utilities.Logger
import ucar.nc2

object runtime {
  def printMemoryUsage(logger: Logger) = {
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    val buf = new StringBuilder
    buf ++= "\n--------------------------------- MEMORY USAGE ---------------------------------\n"
    buf ++= "** Used Memory: %d M \n".format((runtime.totalMemory - runtime.freeMemory) / mb)
    buf ++= "** Free Memory: %d M \n".format(runtime.freeMemory / mb)
    buf ++= "** Total Memory: %d M \n".format(runtime.totalMemory / mb)
    buf ++= "** Max Memory: %d M \n".format(runtime.maxMemory / mb)
    buf ++= "** Processors:   " + runtime.availableProcessors
    buf ++= "\n--------------------------------- ------------ ---------------------------------\n"
    logger.info( buf.toString )
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







