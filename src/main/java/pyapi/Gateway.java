package pyapi;

import py4j.ClientServer;
import py4j.GatewayServer;

public class Gateway {

    public static void main(String[] args) {
        ClientServer clientServer = new ClientServer(null);
        // We get an entry point from the Python side
        ICDAS icdas = (ICDAS) clientServer.getPythonServerEntryPoint(new Class[] { ICDAS.class });
        // Java calls Python without ever having been called from Python
        System.out.println(icdas.sayHello());
        System.out.println(icdas.sayHello(2, "Hello World"));
        clientServer.shutdown();
    }
}