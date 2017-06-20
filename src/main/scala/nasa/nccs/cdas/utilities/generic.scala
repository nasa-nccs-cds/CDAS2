package nasa.nccs.cdas.utilities
import nasa.nccs.utilities.Logger
import ucar.nc2

object runtime {
  def printMemoryUsage(logger: Logger) = {
    val mb = 1024 * 1024
    val gb = mb * 1024.0
    val runtime = Runtime.getRuntime
    val buf = new StringBuilder
    buf ++= "\n--------------------------------- MEMORY USAGE ---------------------------------\n"
    buf ++= "** Used Memory: %.2f G \n".format((runtime.totalMemory - runtime.freeMemory) / gb)
    buf ++= "** Free Memory: %.2f G \n".format(runtime.freeMemory / gb)
    buf ++= "** Total Memory: %.2f G \n".format(runtime.totalMemory / gb)
    buf ++= "** Max Memory: %.2f G \n".format(runtime.maxMemory / gb)
    buf ++= "** Processors:   " + runtime.availableProcessors
    buf ++= "\n--------------------------------- ------------ ---------------------------------\n"
    logger.info( buf.toString )
  }

  def printMemoryUsage = {
    val mb = 1024 * 1024
    val gb = mb * 1024.0
    val runtime = Runtime.getRuntime
    println("--------------------------------- MEMORY USAGE ---------------------------------")
    println("** Used Memory: %.2f G ".format((runtime.totalMemory - runtime.freeMemory) / gb))
    println("** Free Memory: %.2f G ".format(runtime.freeMemory / gb))
    println("** Total Memory: %.2f G ".format(runtime.totalMemory / gb))
    println("** Max Memory: %.2f G ".format(runtime.maxMemory / gb))
    println("** Processors:   " + runtime.availableProcessors )
    println("--------------------------------- ------------ ---------------------------------")
  }
}







