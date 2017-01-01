import subprocess, shlex, os
request_port = 4356
response_port = 4357
cdas_startup = "cdas2 bind {0} {1} -J-Xmx32000M -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:MaxPermSize=800M".format( request_port, response_port)
process = subprocess.Popen(shlex.split(cdas_startup))
print "Staring CDAS with command: {0}\n".format(cdas_startup)
