#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import time
import zmq, numpy as np

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

data = socket.recv()
print( "Received binary data: %s" % ( ' '.join([str(ord(a)) for a in data]) ) )
dt = np.dtype( np.float32 ).newbyteorder('>')
nparray = np.frombuffer( data, dtype=dt )

print( "Float data: " )
for x in np.nditer(nparray):
    print x
#  Do some 'work'
time.sleep(1)

#  Send reply back to client
socket.send(b"World")