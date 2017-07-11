package nasa.nccs.cdas.portal;
import com.google.common.io.Files;
import nasa.nccs.cdas.workers.python.PythonWorkerPortal;
import nasa.nccs.utilities.Logger;
import org.apache.commons.lang.StringUtils;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.CDASLogManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;


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
                try{
                    request_socket.connect(String.format("tcp://localhost:%d", request_port));
                    logger.info(String.format("Connected request socket on port: %d", request_port));
                } catch (Exception err ) { logger.error( String.format("Error initializing request socket on port %d: %s", request_port, err ) ); }
                try{
                    response_socket.connect(String.format("tcp://localhost:%d", response_port));
                    logger.info(String.format("Connected response socket on port: %d", response_port));
                } catch (Exception err ) { logger.error( String.format("Error initializing response socket on port %d: %s", response_port, err ) ); }
            } else {
                try{
                    request_socket.bind(String.format("tcp://*:%d", request_port));
                    logger.info(String.format("Bound request socket to port: %d", request_port));
                } catch (Exception err ) { logger.error( String.format("Error initializing request socket on port %d: %s", request_port, err ) ); }
                try{
                    response_socket.bind(String.format("tcp://*:%d", response_port));
                    logger.info( String.format("Bound response socket to port: %d", response_port) );
                } catch (Exception err ) { logger.error( String.format("Error initializing response socket on port %d: %s", response_port, err ) ); }
            }
        } catch (Exception err ) {
            logger.error( String.format("\n-------------------------------\nCDAS Init error: %s -------------------------------\n", err ) );
        }
    }

    public void sendResponse( String rId, String response  ) {
        List<String> request_args = Arrays.asList( rId, "response", response );
        response_socket.send( StringUtils.join( request_args,  "!" ).getBytes(), 0);
        logger.info( " Sent response: " + rId );
    }
    public void sendArrayData( String rid, int[] origin, int[] shape, byte[] data, Map<String, String> metadata ) {
        logger.debug( String.format("Portal: Sending response data to client for rid %s, nbytes=%d", rid, data.length ));
        List<String> array_header_fields = Arrays.asList( "array", rid, ia2s(origin), ia2s(shape), m2s(metadata), "1" );
        String array_header = StringUtils.join(array_header_fields,"|");
        List<String> header_fields = Arrays.asList( rid,"array", array_header );
        String header = StringUtils.join(header_fields,"!");
        logger.debug("Sending header: " + header);
        sendDataPacket( header, data );
    }

    public String sendFile( String rId, String name, String filePath ) {
        logger.debug( String.format("Portal: Sending file data to client for %s, filePath=%s", name, filePath ));
        File file = new File(filePath);
        String[] file_header_fields = { "array", rId, name, file.getName() };
        String file_header = StringUtils.join( file_header_fields, "|" );
        List<String> header_fields = Arrays.asList( rId,"file", file_header );
        String header = StringUtils.join(header_fields,"!");
        try {
            byte[] data = Files.toByteArray(file);
            sendDataPacket(header, data);
            logger.debug("Done sending file data packet: " + header);
        } catch ( IOException ex ) {
            logger.info( "Error sending file : " + filePath + ": " + ex.toString() );
        }
        return file_header_fields[3];
    }

    public void sendDataPacket( String header, byte[] data ) {
        response_socket.send(header.getBytes(), 0 );
        response_socket.send(data, 0 );
    }
    public abstract void postArray( String header, byte[] data );
    public abstract void execUtility( String[] utilSpec );

    public abstract void execute( String[] taskSpec );
    public abstract void shutdown();
    public abstract void getCapabilities( String[] utilSpec );
    public abstract void describeProcess( String[] utilSpec );

    public String getHostInfo() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return String.format( "%s (%s)", ip.getHostName(), ip.getHostAddress() );
        } catch (UnknownHostException e) { return "UNKNOWN"; }
    }

    public void run() {

        while( active ) try {
            logger.info( String.format( "Listening for requests on port: %d, host: %s",  request_port, getHostInfo() ) );
            String request_header = new String(request_socket.recv(0)).trim();
            String[] parts = request_header.split("[!]");
            logger.info( String.format( "  ###  Processing %s request: %s",  parts[1], request_header ) );
            if( parts[1].equals("array") ) {
                logger.info("Waiting for result data ");
                byte[] data = request_socket.recv(0);
                postArray(request_header, data);
            } else if( parts[1].equals("execute") ) {
                execute( parts );
            } else if( parts[1].equals("util") ) {
                execUtility( parts );
            } else if( parts[1].equals("quit") || parts[1].equals("shutdown") ) {
                term();
            } else if( parts[1].equals("getCapabilities") ) {
                getCapabilities( parts );
            } else if( parts[1].equals("describeProcess") ) {
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
        logger.info( "CDAS Shutdown");
        active = false;
        PythonWorkerPortal.getInstance().quit();
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
        try { response_socket.close(); }  catch ( Exception ex ) { ; }
        shutdown();
    }

    public String ia2s( int[] array ) { return Arrays.toString(array).replaceAll("\\[|\\]|\\s", ""); }
    public String sa2s( String[] array ) { return StringUtils.join(array,","); }
    public String m2s( Map<String, String> metadata ) {
        ArrayList<String> items = new ArrayList<String>();
        for (Map.Entry<String,String> entry : metadata.entrySet() ) {
            items.add( entry.getKey() + ":" + entry.getValue() );
        }
        return StringUtils.join(items,";" );
    }
}
