package nasa.nccs.cdas.workers;
import nasa.nccs.cdapi.data.ArrayBase;
import nasa.nccs.cdapi.data.HeapFltArray;
import org.apache.commons.lang.ArrayUtils;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.Logger;
import ucar.ma2.DataType;
import ucar.ma2.Array;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;

public abstract class Worker {
    int BASE_PORT = 2336;
    ZMQ.Socket request_socket = null;
    ConcurrentLinkedQueue<TransVar> results = null;
    ConcurrentLinkedQueue<String> messages = null;
    Process process = null;
    ResultThread resultThread = null;
    protected Logger logger = null;
    protected int result_port = -1;
    protected int request_port = -1;
    private String errorCondition = null;
    private String withData = "1";
    private String withoutData = "0";
    private long requestTime = 0;
    ByteBuffer byteBuffer = java.nio.ByteBuffer.allocate(4);

    static int bindSocket( ZMQ.Socket socket, int init_port ) {
        int test_port = init_port;
        while( true ) {
            try {
                socket.bind("tcp://*:" + String.valueOf(test_port));
                break;
            } catch (Exception err ) {
                test_port = test_port + 1;
            }
        }
        return test_port;
    }

    public int id() { return request_port; }

    private void postInfo( String info ) {
        logger.info( "Posting info from worker: " + info );
        messages.add( info );
    }

    private void addResult( String result_header, byte[] data ) {
        String elapsedTime = String.valueOf( ( System.currentTimeMillis() - requestTime )/1000.0 );
        logger.info( "*********************************\n Caching result from worker: " + result_header+ ", data size = " + data.length  + ", Worker time = " + elapsedTime + "\n*********************************\n");
        results.add( new TransVar( result_header, data ) );
    }

    private void invalidateRequest( String errorMsg ) { errorCondition = errorMsg; }

    public TransVar getResult() throws Exception {
        logger.debug( "Waiting for result to appear from worker");
        while( true ) {
            if( errorCondition != null ) {
                throw new Exception( errorCondition );
            }
            TransVar result = results.poll();
            if( result == null ) try { Thread.sleep(100); } catch( Exception err ) { return null; }
            else {
                return result;
            }
        }
    }

    public String getMessage() {
        logger.debug( "Waiting for message to be posted from worker");
        while( errorCondition == null ) {
            String message = messages.poll();
            if( message == null ) try { Thread.sleep(100); } catch( Exception err ) { return null; }
            else { return message; }
        }
        return null;
    }

    public class ResultThread extends Thread {
        ZMQ.Socket result_socket = null;
        int port = -1;
        boolean active = true;
        public ResultThread( int base_port, ZMQ.Context context ) {
            result_socket = context.socket(ZMQ.PULL);
            port = bindSocket(result_socket,base_port);
        }
        public void run() {
            while( active ) try {
                String result_header = new String(result_socket.recv(0)).trim();
                String[] parts = result_header.split("[|]");
                logger.info( "Received result header from worker: " + result_header );
                String[] mtypes = parts[0].split("[-]");
                String mtype = mtypes[0];
                int mtlen = parts[0].length();
                String pid = mtypes[1];
                if( mtype.equals("array") ) {
                    logger.debug("Waiting for result data ");
                    byte[] data = result_socket.recv(0);
                    addResult(result_header, data);
                } else if( mtype.equals("info") ) {
                    postInfo( result_header.substring(mtlen+1) );
                } else if( mtype.equals("error") ) {
                    logger.error("Python worker {0} signaled error: {1}\n".format( pid, parts[1]) );
                    invalidateRequest(result_header.substring(mtlen+1));
                    quit();
                } else {
                    logger.info( "Unknown result message type: " + parts[0] );
                }
            } catch ( java.nio.channels.ClosedSelectorException ex ) {
                logger.info( "Result Socket closed." );
                active = false;
            } catch ( Exception ex ) {
                logger.error( "Error in ResultThread: " + ex.toString() );
                ex.printStackTrace();
                term();
            }
        }
        public void term() {
            active = false;
            try { result_socket.close(); }  catch ( Exception ex ) { ; }
        }
    }

    public Worker( ZMQ.Context context, Logger _logger ) {
        logger = _logger;
        results = new ConcurrentLinkedQueue();
        messages = new ConcurrentLinkedQueue();
        request_socket = context.socket(ZMQ.PUSH);
        request_port = bindSocket( request_socket, BASE_PORT );
        resultThread = new ResultThread( request_port + 1, context );
        resultThread.start();
        result_port = resultThread.port;
        logger.info( String.format("Starting Worker, ports: %d %d",  request_port, result_port ) );
        byteBuffer.putFloat( 0, Float.MAX_VALUE );
    }

    @Override
    public void finalize() { quit(); }

    public void sendDataPacket( String header, byte[] data ) {
        logger.debug("Sending header: " + header);
        request_socket.send(header.getBytes(), 0 );
        logger.debug( String.format( "Sending data, nbytes = %d", data.length ) );
        request_socket.send(data, 0 );
    }

    public void quit() {
        logger.debug("Sending Quit request" );
        request_socket.send( "util|quit".getBytes(), 0 );
        try { resultThread.term();  }  catch ( Exception ex ) { ; }
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
    }

    public void getCapabilites() {
        request_socket.send("util|capabilities".getBytes(), 0);
    }

    public void sendRequestInput( String id, HeapFltArray array ) {
        if( array.hasData() )  {
            _sendArrayData( id, array.origin(), array.shape(), array.toByteArray(), array.mdata() );
            scala.Option<float[]> weightsOpt = array.weights();
            if( weightsOpt.isDefined() ) {
                float[] weights = weightsOpt.get();
                int[] shape = Stream.of( array.attr("wshape").split(",") ).mapToInt(Integer::parseInt).toArray();
//                int[] shape = { weights.length };
                byte[] weight_data = ArrayUtils.addAll( Array.factory(DataType.FLOAT, shape, weights ).getDataAsByteBuffer().array(), byteBuffer.array() );
                String[] idtoks =  id.split("-");
                idtoks[0] = idtoks[0] + "_WEIGHTS_";
                _sendArrayData( String.join("-", idtoks ), array.origin(), shape, weight_data, array.mdata()  );
            }
        }
        else _sendArrayMetadata( id, array.origin(), array.shape(), array.mdata() );
    }

    public void sendArrayData( String id, HeapFltArray array ) {
        _sendArrayData( id, array.origin(), array.shape(), array.toByteArray(), array.mdata() );
    }

    public void sendArrayMetadata( String id, HeapFltArray array ) {
        _sendArrayMetadata( id, array.origin(), array.shape(), array.mdata() );
    }

    private void _sendArrayData( String id, int[] origin, int[] shape, byte[] data, Map<String, String> metadata ) {
        logger.debug( String.format("Kernel: Sending data to worker for input %s, nbytes=%d", id, data.length ));
        List<String> slist = Arrays.asList( "array", id, ia2s(origin), ia2s(shape), m2s(metadata), withData );
        String header = StringUtils.join(slist,"|");
        sendDataPacket( header, data );
    }

    private void _sendArrayMetadata( String id, int[] origin, int[] shape, Map<String, String> metadata ) {
        logger.debug( String.format("Kernel: Sending metadata to worker for input %s", id ));
        List<String> slist = Arrays.asList( "array", id, ia2s(origin), ia2s(shape), m2s(metadata), withoutData );
        String header = StringUtils.join(slist,"|");
        logger.debug("Sending header: " + header);
        request_socket.send(header.getBytes(), 0);
    }

    public void sendRequest( String operation, String[] opInputs, Map<String, String> metadata ) {
        List<String> slist = Arrays.asList(  "task", operation, sa2s(opInputs), m2s(metadata)  );
        String header = StringUtils.join(slist,"|");
        logger.info( "Sending Task Request: " + header );
        requestTime = System.currentTimeMillis();
        request_socket.send(header.getBytes(), 0);
        errorCondition = null;
    }

    public void sendUtility( String request ) {
        List<String> slist = Arrays.asList(  "util", request );
        String header = StringUtils.join(slist,"|");
        logger.debug( "Sending Utility Request: " + header );
        request_socket.send(header.getBytes(), 0);
        logger.debug( "Utility Request Sent!" );
    }

    public String getCapabilities() {
        sendUtility("capabilities");
        return getMessage();
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
