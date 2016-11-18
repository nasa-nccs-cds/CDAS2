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
import org.zeromq.ZMQ;

public class Gateway {
    private int iPartition;
    private ClientServer _clientServer = null;
    private ClientServerBuilder builder = null;
    private Process python_process = null;
    private ICDAS icdas = null;
    private int data_port = -1;
    private int java_port = -1;
    private int python_port = -1;
    private ServerSocket dataServer = null;
    private Socket dataSocket = null;
    ZMQ.Context zmqContext = null;
    ZMQ.Socket zmqDataSocket = null;


    public Gateway( int partIndex ) {
        iPartition = partIndex;
        java_port =   GatewayServer.DEFAULT_PORT + 3 * partIndex;
        python_port = GatewayServer.DEFAULT_PORT + 3 * partIndex + 1;
        data_port = GatewayServer.DEFAULT_PORT + 3 * partIndex + 2;
        zmqContext = ZMQ.context(1);
        builder = new ClientServerBuilder();
        builder.javaPort(java_port).pythonPort(python_port).connectTimeout(100);
        try {
            python_process =  startupPythonWorker( java_port, python_port, data_port );
            zmqDataSocket = zmqContext.socket(ZMQ.REQ);
            zmqDataSocket.bind( String.format( "tcp://localhost:%d", data_port ) );
            System.out.println( String.format("Gateway-%d dataSocket connected on port %d", partIndex, data_port ) );
        } catch ( Exception ex ) {
            System.out.println( "Error starting data socket : " + ex.toString() );
        }
    }

    public int getDataPort( ) { return data_port; }

    public void sendDataViaSocket( byte[] data ) {
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

    public void sendDataZMQ( byte[] data ) {
        try {
            System.out.println( "Gateway-" + iPartition + " sending data, nbytes = " + data.length );
            zmqDataSocket.send( data, 0 );
            System.out.println( "Gateway-" + iPartition + " finished sending data! " );
        } catch ( Exception ex ) {
            System.out.println( "Error sending data : " + ex.toString() );
        }
    }

    public byte[] recvDataViaSocket( int nbytes ) {
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

    public byte[] recvDataZMQ( int nbytes ) {
        byte[] response = null;
        try {
            System.out.println( "Gateway-" + iPartition + " reading data, nbytes = " + nbytes );
            response = zmqDataSocket.recv();
            System.out.println( "Gateway-" + iPartition + " finished reading data! " );

        } catch ( Exception ex ) {
            System.out.println( "Error sending data : " + ex.toString() );
        }
        return response;
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

    public ServerSocket startupPythonWorkerWithServerSocket( int java_port, int python_port ) {
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

    public Process startupPythonWorker( int java_port, int python_port, int data_port ) {
        try {
            String cdas_home = System.getenv("CDAS_HOME_DIR");
            Path path = FileSystems.getDefault().getPath(cdas_home, "python", "pycdas", "icdas.py" );
            Process process = new ProcessBuilder( "python", path.toString(), String.valueOf(iPartition), String.valueOf(java_port), String.valueOf(python_port), String.valueOf( data_port ) ).start();
            System.out.println( "Starting Python Worker on port: " + String.valueOf(python_port) );
            return process;
        } catch ( IOException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
            return null;
        }
    }

    public void shutdown( ) {
        if( _clientServer != null ) { _clientServer.shutdown(); _clientServer = null; }
        if( python_process != null ) { python_process.destroy(); python_process = null; }
        try { dataServer.close(); }  catch ( Exception ex ) { System.out.println( "Error closing data socket: " + ex.toString() ); }
        try { zmqDataSocket.close(); }  catch ( Exception ex ) { System.out.println( "Error closing zmqDataSocket: " + ex.toString() ); }
        try { zmqContext.term(); }  catch ( Exception ex ) { System.out.println( "Error terminating zmqContext: " + ex.toString() ); }
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
