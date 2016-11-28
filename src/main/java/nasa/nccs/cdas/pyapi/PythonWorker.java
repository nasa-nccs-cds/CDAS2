package nasa.nccs.cdas.pyapi;
import org.zeromq.ZMQ;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PythonWorker {
    int BASE_PORT = 2336;
    ZMQ.Socket request_socket = null;
    ConcurrentLinkedQueue<TransVar> results = null;
    Process process = null;
    ResultThread resultThread = null;
    Logger logger = null;
    int index;

    private void addResult( String result_header, byte[] data ) {
        logger.info( "Caching result from worker: " + result_header );
        results.add( new TransVar( result_header, data ) );
    }

    public TransVar getResult() {
        logger.info( "Waiting for result to appear from worker");
        while( true ) {
            if( results.isEmpty() ) try { Thread.sleep(100); } catch( Exception err ) { return null; }
            else { return results.poll(); }
        }
    }

    public class ResultThread extends Thread {
        ZMQ.Socket result_socket = null;
        boolean active = true;
        public ResultThread( int result_port, ZMQ.Context context ) {
            result_socket = context.socket(ZMQ.PULL);
            result_socket.bind("tcp://*:"+ String.valueOf(result_port));
        }
        public void run() {
            while( active ) try {
                String result_header = new String(result_socket.recv(0)).trim();
                String[] parts = result_header.split("[|]");
                logger.info( "Received result header from worker: " + result_header );
                if( parts[0].equals("array") ) {
                    logger.info( "Waiting for result data " );
                    byte[] data = result_socket.recv(0);
                    addResult( result_header, data );
                } else {
                    logger.info( "Unknown result header type: " + parts[0] );
                }
            } catch ( java.nio.channels.ClosedSelectorException ex ) {
                System.out.println( "Result Socket closed." );
                active = false;
            } catch ( Exception ex ) {
                System.out.println( "Error in ResultThread: " + ex.toString() );
                ex.printStackTrace();
                term();
            }
        }
        public void term() {
            active = false;
            try { result_socket.close(); }  catch ( Exception ex ) { ; }
        }
    }

    public PythonWorker( int worker_index, ZMQ.Context context, Logger _logger ) {
        index = worker_index;
        logger = _logger;
        int request_port = BASE_PORT + worker_index * 2;
        int result_port = request_port + 1;
        results = new ConcurrentLinkedQueue();
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
        List<String> slist = Arrays.asList(  "task", operation, sa2s(inputs), m2s(metadata)  );
        String header = String.join("|", slist);
        System.out.println( "Sending Task Request: " + header );
        request_socket.send(header);
    }

    public void sendShutdown( ) {
        List<String> slist = Arrays.asList(  "quit", "0"  );
        String header = String.join("|", slist);
        System.out.println( "Sending Quit Request: " + header );
        request_socket.send(header);
    }

    private Process startup( int request_port, int result_port ) {
        try {
            String cdas_home = System.getenv("CDAS_HOME_DIR");
            Path path = FileSystems.getDefault().getPath(cdas_home, "python", "pycdas", "worker.py" );
            Path process_root = FileSystems.getDefault().getPath( cdas_home, "python" );
            Path log_path = FileSystems.getDefault().getPath( System.getProperty("user.home"), ".cdas", String.format("python-worker-%d.log",index) );
            ProcessBuilder pb = new ProcessBuilder( "python", path.toString(), String.valueOf(index), String.valueOf(request_port), String.valueOf(result_port) );
            Map<String, String> env = pb.environment();
            env.put("PYTHONPATH", env.get("PYTHONPATH") + ":" + process_root.toString() );
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
        sendShutdown();
        try { resultThread.term();  }  catch ( Exception ex ) { ; }
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
    }

    public String ia2s( int[] array ) { return Arrays.toString(array).replaceAll("\\[|\\]|\\s", ""); }
    public String sa2s( String[] array ) { return String.join(",",array); }
    public String m2s( Map<String, String> metadata ) {
        ArrayList<String> items = new ArrayList<String>();
        for (Map.Entry<String,String> entry : metadata.entrySet() ) {
            items.add( entry.getKey() + ":" + entry.getValue() );
        }
        return String.join( ";", items );
    }
}
