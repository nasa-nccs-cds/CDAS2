package nasa.nccs.cdas.pyapi;
import py4j.ClientServer;
import py4j.ClientServer.ClientServerBuilder;
import py4j.GatewayServer;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Gateway {
    private int iPartition;
    private ClientServer _clientServer = null;
    private ClientServerBuilder builder = null;
    private Process python_process = null;
    private ICDAS icdas = null;
    private ServerSocket dataServer = null;
    private Socket dataSocket = null;


    public Gateway( int partIndex ) {
        iPartition = partIndex;
        int java_port =   GatewayServer.DEFAULT_PORT + 3 * partIndex;
        int python_port = GatewayServer.DEFAULT_PORT + 3 * partIndex + 1;
        builder = new ClientServerBuilder();
        builder.javaPort(java_port).pythonPort(python_port).connectTimeout(100);
        try {
            dataServer = startupPythonWorker( java_port, python_port );
            dataSocket = dataServer.accept();
            System.out.println( "Gateway-" + partIndex + " dataSocket connectd on port " + dataSocket.getPort() );
        } catch ( Exception ex ) {
            System.out.println( "Error starting data socket : " + ex.toString() );
        }
    }

    public int getDataPort( ) { return dataSocket.getPort(); }

    public void sendData( byte[] data ) {
        try {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(dataSocket.getOutputStream()));
            System.out.println( "Gateway-" + iPartition + " sending data, nbytes = " + data.length );
            out.write( data, 0, data.length );
            System.out.println( "Gateway-" + iPartition + " finished sending data! " );
            out.close();
        } catch ( Exception ex ) {
            System.out.println( "Error sending data : " + ex.toString() );
        }
    }

    public byte[] recvData( int nbytes ) {
        byte[] dataBuffer = null;
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(dataSocket.getInputStream()));
            System.out.println( "Gateway-" + iPartition + " reading data, nbytes = " + nbytes );
            dataBuffer = new byte[nbytes];
            in.readFully( dataBuffer );
            System.out.println( "Gateway-" + iPartition + " finished reading data! " );
            in.close();
        } catch ( Exception ex ) {
            System.out.println( "Error sending data : " + ex.toString() );
        }
        return dataBuffer;
    }

    private void printPythonLog() {
        try {
            Path path = FileSystems.getDefault().getPath(System.getProperty("user.home"), ".cdas", String.format("pycdas-%d.log", iPartition));
            BufferedReader br = new BufferedReader(new FileReader(path.toString()));
            System.out.println( "\tPYTHON LOG: PARTITION-" + String.valueOf(iPartition) );
            String line = br.readLine();
            while (line != null) {
                System.out.println( line );
                line = br.readLine();
            }
            br.close();
        } catch ( IOException ex ) {
            System.out.println( "Error reading log file : " + ex.toString() );
        }
    }

    public ICDAS getInterface() {
        if( _clientServer == null ) { _clientServer = builder.build(); }
        if( icdas == null ) { icdas = (ICDAS) _clientServer.getPythonServerEntryPoint(new Class[]{ICDAS.class}); }
        return icdas;
    }

    public ServerSocket startupPythonWorker( int java_port, int python_port ) {
        try {
            String cdas_home = System.getenv("CDAS_HOME_DIR");
            Path path = FileSystems.getDefault().getPath(cdas_home, "python", "pycdas", "icdas.py" );
            ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"));
            // Close the socket if no connection in 3 seconds
            serverSocket.setSoTimeout(5000);
            int data_port = serverSocket.getLocalPort();
            python_process = new ProcessBuilder( "python", path.toString(), String.valueOf(iPartition), String.valueOf(java_port), String.valueOf(python_port), String.valueOf( data_port ) ).start();
            System.out.println( "Starting Python Worker on port: " + String.valueOf(python_port) );
            return serverSocket;
        } catch ( IOException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
            return null;
        }
    }

    public void shutdown( ) {
        if( _clientServer != null ) { _clientServer.shutdown(); _clientServer = null; }
        if( python_process != null ) { python_process.destroy(); python_process = null; }
        try { dataServer.close(); }  catch ( Exception ex ) { System.out.println( "Error closing data socket: " + ex.toString() ); }
        printPythonLog();
    }

//    public static void main(String[] args) {
//        for(int index=0; index<1; index++) {
//            Gateway gateway = new Gateway(index);
//            try {
//                ICDAS icdas = gateway.getInterface();
//                System.out.println(icdas.sayHello(2, "Hello World"));
//                int[] shape = { 1, 2 };
//                int[] origin = { 0, 0 };
//                float[] data = { 0f, 1f };
//                TransArray trans_data = new TransArray( "", shape, origin, data, Float.MAX_VALUE );
////                System.out.println( icdas.sendData( Arrays.asList(trans_data) ) );
//            } catch ( Exception ex ) {
//                System.out.println( "Gateway error: " + ex.toString() );
//                if( ex.getCause() != null ) { System.out.println( "Cause: " + ex.getCause().toString() ); }
//                ex.printStackTrace();
//            } finally { gateway.shutdown(); }
//        }
//    }
}
