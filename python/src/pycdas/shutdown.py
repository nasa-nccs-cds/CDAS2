import subprocess, signal, os
p = subprocess.Popen(['ps', '-fu', '$USER', "|", "grep", "python"], stdout=subprocess.PIPE)
out, err = p.communicate()

print "Killing zombie workers: "
for line in out.splitlines():
   if not ('shutdown' in line):
     print " >> " + line
     pid = int(line.split()[1])
     os.kill(pid, signal.SIGKILL)