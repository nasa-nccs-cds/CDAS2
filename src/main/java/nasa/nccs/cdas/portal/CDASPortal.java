package nasa.nccs.cdas.portal;
import nasa.nccs.cdas.workers.python.PythonWorkerPortal;
import nasa.nccs.utilities.Logger;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.CDASLogManager;
import java.util.Arrays;

public abstract class CDASPortal {
    public enum ConnectionMode { BIND, CONNECT };
    protected ZMQ.Context zmqContext = null;
    protected ZMQ.Socket request_socket = null;
    protected ZMQ.Socket response_socket = null;
    protected int request_port = -1;
    protected int response_port = -1;
    protected Logger logger = CDASLogManager.getCurrentLogger();
    private boolean active = true;

    protected CDASPortal( ConnectionMode mode, int _request_port, int _response_port ) {
        try {
            request_port = _request_port;
            response_port = _response_port;
            zmqContext = ZMQ.context(1);
            request_socket = zmqContext.socket(ZMQ.PULL);
            response_socket = zmqContext.socket(ZMQ.PUSH);
            if( mode == ConnectionMode.CONNECT ) {
                request_socket.connect(String.format("tcp://localhost:%d", request_port));
                logger.info(String.format("Connected request socket on port: %d", request_port));
                response_socket.connect(String.format("tcp://localhost:%d", response_port));
                logger.info(String.format("Connected response socket on port: %d", response_port));
            } else {
                request_socket.bind(String.format("tcp://*:%d", request_port));
                logger.info(String.format("Bound request socket to port: %d", request_port));
                response_socket.bind(String.format("tcp://*:%d", response_port));
                logger.info(String.format("Bound response socket to port: %d", response_port));
            }
        } catch (Exception err ) {
            logger.error( String.format("\n-------------------------------\nCDAS Init error: %s -------------------------------\n", err ) );
        }
    }

    public void sendResponse( String response  ) {
        response_socket.send( String.join("!","response", response ) );
        logger.info( " Sent response: " + response );
    }

    public abstract void postArray( String header, byte[] data );
    public abstract void execUtility( String[] utilSpec );

    public abstract void execute( String[] taskSpec );
    public abstract void getCapabilities( String[] utilSpec );
    public abstract void describeProcess( String[] utilSpec );

    public void run() {
        while( active ) try {
            logger.info( String.format( "Listening for requests on port: %d",  request_port ) );
            String request_header = new String(request_socket.recv(0)).trim();
            String[] parts = request_header.split("[!]");
            logger.info( String.format( "  ###  Processing %s request: %s",  parts[0], request_header ) );
            if( parts[0].equals("array") ) {
                logger.info("Waiting for result data ");
                byte[] data = request_socket.recv(0);
                postArray(request_header, data);
            } else if( parts[0].equals("execute") ) {
                execute( parts );
            } else if( parts[0].equals("util") ) {
                execUtility( parts );
            }else if( parts[0].equals("quit") || parts[0].equals("shutdown") ) {
                term();
            } else if( parts[0].equals("getCapabilities") ) {
                getCapabilities( parts );
            } else if( parts[0].equals("describeProcess") ) {
                describeProcess( parts );
            } else {
                logger.info( "Unknown request header type: " + parts[0] );
            }
        } catch ( java.nio.channels.ClosedSelectorException ex ) {
            logger.info( "Request Socket closed." );
            active = false;
        } catch ( Exception ex ) {
            logger.error( "Error in Request: " + ex.toString() );
            ex.printStackTrace();
            term();
        }
    }

    public void term() {
        active = false;
        PythonWorkerPortal.getInstance().quit();
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
        try { response_socket.close(); }  catch ( Exception ex ) { ; }
    }

}
