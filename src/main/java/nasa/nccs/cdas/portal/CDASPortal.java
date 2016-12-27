package nasa.nccs.cdas.portal;
import org.slf4j.Logger;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.CDASLogManager;

import java.util.Arrays;

public abstract class CDASPortal {
    protected ZMQ.Context zmqContext = null;
    protected ZMQ.Socket request_socket = null;
    protected ZMQ.Socket response_socket = null;
    protected int request_port = -1;
    protected int response_port = -1;
    protected Logger logger = CDASLogManager.getCurrentLogger();
    private boolean active = true;

    protected CDASPortal( int _request_port, int _response_port ) {
        try {
            request_port = _request_port;
            response_port = _response_port;
            zmqContext = ZMQ.context(1);
            request_socket = zmqContext.socket(ZMQ.PULL);
            request_socket.connect(String.format("tcp://localhost:%d", request_port));
            logger.info(String.format("Connected request socket on port: %d", request_port));
            response_socket = zmqContext.socket(ZMQ.PUSH);
            response_socket.connect(String.format("tcp://localhost:%d", response_port));
            logger.info(String.format("Connected response socket on port: %d", response_port));
        } catch (Exception err ) {
            logger.error( String.format("\n-------------------------------\nCDAS Init error: %s -------------------------------\n", err ) );
        }
    }

    public void sendResponse( String response  ) {
        response_socket.send( response );
        logger.info( " Sent response: " + response );
    }

    abstract void postArray( String header, byte[] data );
    abstract void execUtility( String[] utilSpec );

    abstract void execute( String[] taskSpec );
    abstract void getCapabilities( String[] utilSpec );
    abstract void describeProcess( String[] utilSpec );

    public void run() {
        while( active ) try {
            logger.info( String.format( "Listening for requests on port: %d",  request_port ) );
            String request_header = new String(request_socket.recv(0)).trim();
            String[] parts = request_header.split("[|]");
            logger.info( "Received request header from portal: " + request_header );
            if( parts[0].equals("array") ) {
                logger.info("Waiting for result data ");
                byte[] data = request_socket.recv(0);
                postArray(request_header, data);
            } else if( parts[0].equals("execute") ) {
                execute( Arrays.copyOfRange( parts, 1, parts.length) );
            } else if( parts[0].equals("util") ) {
                execUtility( Arrays.copyOfRange( parts, 1, parts.length) );
            } else if( parts[0].equals("getCapabilities") ) {
                getCapabilities( Arrays.copyOfRange( parts, 1, parts.length) );
            } else if( parts[0].equals("describeProcess") ) {
                describeProcess( Arrays.copyOfRange( parts, 1, parts.length) );
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
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
        try { response_socket.close(); }  catch ( Exception ex ) { ; }
    }

}
