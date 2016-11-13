package nasa.nccs.cdas.pyapi;

import py4j.ClientServer;
import py4j.GatewayServer;

public class Gateway {

    public static void main(String[] args) {
        ClientServer clientServer = new ClientServer(null);
        // We get an entry point from the Python side
        ICDAS icdas = (ICDAS) clientServer.getPythonServerEntryPoint(new Class[] { ICDAS.class });
        // Java calls Python without ever having been called from Python
        System.out.println(icdas.sayHello(2, "Hello World"));
        int[] shape = {1,2,3};
        float[] data = { 1.0f,2.0f,3.0f };
        System.out.println( icdas.sendArray( data, shape ) );
        clientServer.shutdown();
    }
}