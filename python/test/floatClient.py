#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq, numpy as np

context = zmq.Context()

#  Socket to talk to server
print("Connecting to hello world server")
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")
# val byte_data = input_array.toUcarFloatArray.getDataAsByteBuffer().array()

databuff = np.getbuffer( np.array([ 2.5, 3.1, 1.1, 0.1]) )
data = [ databuff[i] for i in range(0,4) ]
print("Sending request %d %d %d %d" % ( data[0], data[1], data[2], data[3] ) )
socket.send( data )

#  Get the reply.
message = socket.recv()
print("Received reply [ %s ]" % (message))