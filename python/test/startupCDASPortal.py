import subprocess, shlex, os
request_port = 4356
response_port = 4357
CDAS_MAX_MEM = os.environ.get( 'CDAS_MAX_MEM', '32000M' )

try:
    cdas_startup = "cdas2 bind {0} {1} -J-Xmx{2} -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC".format( request_port, response_port, CDAS_MAX_MEM )
    process = subprocess.Popen(shlex.split(cdas_startup))
    print "Staring CDAS with command: {0}\n".format(cdas_startup)
    process.wait()
except KeyboardInterrupt as ex:
    print "  ----------------- CDAS TERM ----------------- "
    process.kill()