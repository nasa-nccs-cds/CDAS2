package nasa.nccs.cdas.pyapi;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PythonWorker {
    int BASE_PORT = 2336;
    ZMQ.Socket request_socket = null;
    ConcurrentLinkedQueue<String> results = null;
    Process process = null;
    ResultThread resultThread = null;
    int index;

    public class ResultThread extends Thread {
        ZMQ.Socket result_socket = null;
        boolean active = true;
        public ResultThread( int result_port, ZMQ.Context context ) {
            result_socket = context.socket(ZMQ.PULL);
            result_socket.bind("tcp://*:"+ String.valueOf(result_port));
        }
        public void run() {
            while( active ) try {
                String result_header = new String( result_socket.recv(0) ).trim();
                results.add( result_header );
            } catch ( Exception ex ) {
                System.out.println( "Error in ResultThread: " + ex.toString() );
                term();
            }
        }
        public void term() {
            active = false;
            try { result_socket.close();  result_socket = null; }  catch ( Exception ex ) { System.out.println( "Error closing result_socket: " + ex.toString() ); }
        }
    }

    public PythonWorker( int worker_index, ZMQ.Context context ) {
        index = worker_index;
        int request_port = BASE_PORT + worker_index * 2;
        int result_port = request_port + 1;
        request_socket = context.socket(ZMQ.PUSH);
        request_socket.bind("tcp://*:" + String.valueOf(request_port) );
        process = startup( request_port, result_port );
        resultThread = new ResultThread( result_port, context );
        resultThread.start();
    }

    public void sendDataPacket( String header, byte[] data ) {
        request_socket.send(header);
        request_socket.send(data);
    }

    public void quit() {
        request_socket.send("quit");
        shutdown();
    }

    public void sendArrayData( String id, int[] origin, int[] shape, byte[] data, Map<String, String> metadata ) {
        List<String> slist = Arrays.asList( "array", id, ia2s(origin), ia2s(shape), m2s(metadata) );
        String header = String.join("|", slist);
        System.out.println("Sending header: " + header);
        sendDataPacket( header, data );
    }

    public void sendRequest( String operation, String[] inputs, Map<String, String> metadata ) {
        String header = "|".join( "task", operation, sa2s(inputs), m2s(metadata) );
        request_socket.send(header);
    }

    private Process startup( int request_port, int result_port ) {
        try {
            String cdas_home = System.getenv("CDAS_HOME_DIR");
            Path path = FileSystems.getDefault().getPath(cdas_home, "python", "pycdas", "worker.py" );
            Path process_root = FileSystems.getDefault().getPath( cdas_home, "python" );
            Path log_path = FileSystems.getDefault().getPath( System.getProperty("user.home"), ".cdas", String.format("python-worker-%d.log",index) );
            ProcessBuilder pb = new ProcessBuilder( "python", path.toString(), String.valueOf(index), String.valueOf(request_port), String.valueOf(result_port) );
            pb.directory( process_root.toFile() );
            pb.redirectErrorStream( true );
            pb.redirectOutput( ProcessBuilder.Redirect.appendTo( log_path.toFile() ));
            System.out.println( "Starting Python Worker " + String.valueOf(index) );
            return pb.start();
        } catch ( IOException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
            return null;
        }
    }

    public void shutdown( ) {
        try { request_socket.close(); request_socket = null; }  catch ( Exception ex ) { System.out.println( "Error closing request_socket: " + ex.toString() ); }
        try { resultThread.term();  }  catch ( Exception ex ) { System.out.println( "Error closing result_socket: " + ex.toString() ); }
        try { process.destroy(); process = null; } catch ( Exception ex ) { System.out.println( "Error killing worker process: " + ex.toString() ); }
    }

    public String ia2s( int[] array ) { return Arrays.toString(array).replaceAll("\\[|\\]|\\s", ""); }
    public String sa2s( String[] array ) { return Arrays.toString(array).replaceAll("\\[|\\]|\\s", ""); }
    public String m2s( Map<String, String> metadata ) {
        ArrayList<String> items = new ArrayList<String>();
        for (Map.Entry<String,String> entry : metadata.entrySet() ) {
            items.add( entry.getKey() + ":" + entry.getValue() );
        }
        return Arrays.toString( items.toArray() ).replaceAll("\\[|\\]|\\s", "");
    }
}
