package nasa.nccs.cdas.workers.test;

//
//  Hello World client in Java
//  Connects REQ socket to tcp://localhost:5555
//  Sends "Hello" to server, expects "World" back
//

import org.zeromq.ZMQ;

public class floatClient {

    public static void run( byte[] data ) {
        ZMQ.Context context = ZMQ.context(1);

        //  Socket to talk to server
        System.out.println("Connecting to hello world server…");

        ZMQ.Socket requester = context.socket(ZMQ.REQ);
        requester.connect("tcp://localhost:5555");

        System.out.println("Sending Data ");
        requester.send(data, 0);

        byte[] reply = requester.recv(0);
        System.out.println( "Received " + new String(reply) );

        requester.close();
        context.term();
    }
}