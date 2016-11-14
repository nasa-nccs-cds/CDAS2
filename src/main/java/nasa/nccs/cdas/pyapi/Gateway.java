package nasa.nccs.cdas.pyapi;
import py4j.ClientServer;
import py4j.ClientServer.ClientServerBuilder;
import py4j.GatewayServer;
import nasa.nccs.cdas.pyapi.TransData;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Gateway {
    private int base_port = 8200;
    private ClientServer _clientServer = null;
    private ClientServerBuilder builder = null;
    private Process python_process = null;
    private ICDAS icdas = null;


    public Gateway( int partIndex ) {
        int python_port = (partIndex >= 0) ? base_port + 2 * partIndex : GatewayServer.DEFAULT_PYTHON_PORT;
        int java_port = (partIndex >= 0) ? base_port + 2 * partIndex + 1 : GatewayServer.DEFAULT_PORT;
        ClientServerBuilder builder = new ClientServerBuilder();
        builder.javaPort(java_port);
        builder.pythonPort(python_port);
        startupPythonWorker(java_port, python_port);
        builder.connectTimeout(100);
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
            python_process = new ProcessBuilder( "python", path.toString(), String.valueOf(java_port), String.valueOf(python_port) ).start();
            Thread.sleep(100);
        } catch ( IOException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
        } catch ( InterruptedException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
        }
    }

    public void shutdown( ) {
        if( _clientServer != null ) { _clientServer.shutdown(); _clientServer = null; }
        if( python_process != null ) { python_process.destroy(); python_process = null; }
    }

    public static void main(String[] args) {
        List<Gateway>  gateways = new ArrayList<Gateway>();
        try {
            for(int index=0; index<3; index++) {
                Gateway gateway = new Gateway(index);
                ICDAS icdas = gateway.getInterface();
                System.out.println(icdas.sayHello(2, "Hello World"));
                int[] shape = { index+1, index+2 };
                float[] data = { index*2f, index*3f };
                TransData trans_data = new TransData( shape, data );
                System.out.println( icdas.sendData(trans_data) );
                gateways.add( gateway );
            }
        } catch ( Exception ex ) {
            System.out.println( "Gateway error: " + ex.toString() );
            if( ex.getCause() != null ) { System.out.println( "Cause: " + ex.getCause().toString() ); }
            ex.printStackTrace();
        }
        for (int i = 0; i < gateways.size(); i++) { gateways.get(i).shutdown(); }
    }
}
