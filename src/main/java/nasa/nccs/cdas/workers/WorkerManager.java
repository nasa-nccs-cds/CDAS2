package nasa.nccs.cdas.workers;
import nasa.nccs.utilities.CDASLogManager;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;

public class WorkerManager {
    int BASE_PORT = 2335;
    ResponderThread responderThread = null;

    private WorkerManager() {
        responderThread = new ResponderThread(BASE_PORT);
        responderThread.start();
    }

    public void setLogger( Logger logger ) { responderThread.setLogger( logger ); }

    private static class SingletonHelper{
        private static final WorkerManager INSTANCE = new WorkerManager();
    }

    public static WorkerManager getInstance(){
        return SingletonHelper.INSTANCE;
    }

    public class ResponderThread extends Thread {
        ZMQ.Context zmqContext = null;
        ZMQ.Socket responder = null;
        Logger _logger = null;
        boolean active = true;
        int index = 1;

        public ResponderThread( int responder_port ) {
            zmqContext = ZMQ.context(1);
            responder = zmqContext.socket(ZMQ.REP);
            responder.bind("tcp://*:"+ String.valueOf(responder_port));
        }

        public void setLogger( Logger logger ) { _logger = logger; }

        public void run() {
            while( active && !isInterrupted() ) try {
                String message = new String(responder.recv(0)).trim();
                if( message.equals("index") ) {
                    String reply = String.valueOf(index);
                    responder.send( reply.getBytes(), 0 );
                    index = index + 1;
                }
            } catch ( java.nio.channels.ClosedSelectorException ex ) {
                if( _logger != null ) { _logger.info( "Result Socket closed." ); }
                active = false;
            } catch ( Exception ex ) {
                if( _logger != null ) { _logger.error( "Error in ResponderThread: " + ex.toString() ); }
                ex.printStackTrace();
                term();
            }
        }

        public void term() {
            active = false;
            try { responder.close(); }  catch ( Exception ex ) { ; }
        }
    }
}
