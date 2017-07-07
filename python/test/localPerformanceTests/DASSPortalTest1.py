from pycdas.portal.cdas import *
import zmq

request_port = 5670
app_host = "10.71.9.11"

context = zmq.Context()
request_socket = context.socket(zmq.PUSH)
request_port1 = request_socket.connect("tcp://{0}:{1}".format( app_host, request_port ) )
print("Connected request socket to server {0} on port: {1}".format( app_host, request_port1 ) )


