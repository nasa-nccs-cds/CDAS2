import subprocess, shlex, os
from psutil import virtual_memory
request_port = 5670
response_port = 5671
MB = 1024 * 1024
mem = virtual_memory()
total_ram = mem.total / MB
CDAS_MAX_MEM = os.environ.get( 'CDAS_MAX_MEM', str( total_ram - 1000 ) + 'M' )

try:
    cdas_startup = "cdas2 bind {0} {1} -J-Xmx{2} -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC".format( request_port, response_port, CDAS_MAX_MEM )
    process = subprocess.Popen(shlex.split(cdas_startup))
    print "Staring CDAS with command: {0}\n".format(cdas_startup)
    process.wait()
except KeyboardInterrupt as ex:
    print "  ----------------- CDAS TERM ----------------- "
    process.kill()