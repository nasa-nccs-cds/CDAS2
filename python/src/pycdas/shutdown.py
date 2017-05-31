import subprocess, signal, os
p = subprocess.Popen(['pkill', '-u', '$USER', "python"], stdout=subprocess.PIPE)
out, err = p.communicate()
