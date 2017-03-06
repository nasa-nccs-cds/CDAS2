import subprocess, shlex
request_port = 4356
response_port = 4357

try:
    cdas_startup = "cdas2 bind {0} {1} -J-Xmx$CDAS_MAX_MEM -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC".format( request_port, response_port)
    process = subprocess.Popen(shlex.split(cdas_startup))
    print "Staring CDAS with command: {0}\n".format(cdas_startup)
    process.wait()
except KeyboardInterrupt as ex:
    print "  ----------------- CDAS TERM ----------------- "
    process.kill()