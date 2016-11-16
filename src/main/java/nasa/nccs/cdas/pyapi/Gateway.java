package nasa.nccs.cdas.pyapi;
import py4j.ClientServer;
import py4j.ClientServer.ClientServerBuilder;
import py4j.GatewayServer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Gateway {
    private int base_port = 8200;
    private int iPartition;
    private ClientServer _clientServer = null;
    private ClientServerBuilder builder = null;
    private Process python_process = null;
    private ICDAS icdas = null;


    public Gateway( int partIndex ) {
        iPartition = partIndex;
        int python_port = (partIndex >= 0) ? base_port + 2 * partIndex : GatewayServer.DEFAULT_PYTHON_PORT;
        int java_port = (partIndex >= 0) ? base_port + 2 * partIndex + 1 : GatewayServer.DEFAULT_PORT;
        builder = new ClientServerBuilder();
        builder.javaPort(java_port).pythonPort(python_port).connectTimeout(100);
        startupPythonWorker(java_port, python_port);
    }

    private void printPythonLog() {
        try {
            Path path = FileSystems.getDefault().getPath(System.getProperty("user.home"), ".cdas", String.format("pycdas-%d.log", iPartition));
            BufferedReader br = new BufferedReader(new FileReader(path.toString()));
            System.out.println( "PYTHON LOG: PARTITION-" + String.valueOf(iPartition) );
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

    public void  startupPythonWorker( int java_port, int python_port ) {
        try {
            String cdas_home = System.getenv("CDAS_HOME_DIR");
            Path path = FileSystems.getDefault().getPath(cdas_home, "python", "pycdas", "icdas.py" );
            python_process = new ProcessBuilder( "python", path.toString(), String.valueOf(iPartition), String.valueOf(java_port), String.valueOf(python_port) ).start();
            System.out.println( "Starting Python Worker on port: " + String.valueOf(python_port) );
            Thread.sleep(1000);
        } catch ( IOException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
        } catch ( InterruptedException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
        }
    }

    public void shutdown( ) {
        if( _clientServer != null ) { _clientServer.shutdown(); _clientServer = null; }
        if( python_process != null ) { python_process.destroy(); python_process = null; }
        printPythonLog();
    }

    public static void main(String[] args) {
        for(int index=0; index<1; index++) {
            Gateway gateway = new Gateway(index);
            try {
                ICDAS icdas = gateway.getInterface();
                System.out.println(icdas.sayHello(2, "Hello World"));
                int[] shape = { index+1, index+2 };
                int[] origin = { 0, 0 };
                float[] data = { index*2f, index*3f };
                TransArray trans_data = new TransArray( "", shape, origin, data, Float.MAX_VALUE );
                System.out.println( icdas.sendData( Arrays.asList(trans_data) ) );
            } catch ( Exception ex ) {
                System.out.println( "Gateway error: " + ex.toString() );
                if( ex.getCause() != null ) { System.out.println( "Cause: " + ex.getCause().toString() ); }
                ex.printStackTrace();
            } finally { gateway.shutdown(); }
        }
    }
}
