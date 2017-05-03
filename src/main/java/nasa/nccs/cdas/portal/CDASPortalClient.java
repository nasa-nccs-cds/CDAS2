package nasa.nccs.cdas.portal;
import nasa.nccs.utilities.CDASLogManager;
import nasa.nccs.utilities.Logger;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;

class ConnectionMode {
    protected static int BIND = 1;
    protected static int CONNECT = 2;
    protected static int DefaultPort = 4336;


    public static int bindSocket(ZMQ.Socket socket, int port) {
        if (port > 0) {
            socket.bind(String.format("tcp://*:%d", port));
        } else {
            int test_port = DefaultPort;
            while (true) {
                try {
                    socket.bind(String.format("tcp://*:%d", port));
                    return test_port;
                } catch (Exception err) {
                    test_port = test_port + 1;
                }
            }
        }
        return -1;
    }

    public static int connectSocket(ZMQ.Socket socket, String host, int port) {
        socket.connect(String.format("tcp://%s:%d", host, port));
        return port;
    }
}

class ResponseManager extends Thread {
    ZMQ.Socket socket = null;
    Boolean active = true;
    Map<String, String> cached_results = null;
    Map<String, String> cached_arrays = null;
    protected Logger logger = CDASLogManager.getCurrentLogger();

    public ResponseManager(CDASPortalClient portalClient) {
        socket = portalClient.response_socket
        cached_results = new HashMap<String, String>();
        cached_arrays = new HashMap<String, String>();
        setName("CDAS ResponseManager");
        setDaemon(true);
    }


    public void cacheResult(String id, String result) {
        getResults(id).append(result)
    }

    public void getResults(String id) {
        return cached_results.setdefault(id,[])
    }

    public void cacheArray(String id, String array) {
        getArrays(id).append(array);
    }

    public void getArrays(String id) {
        return cached_arrays.setdefault(id,[])
    }

    public void run() {
        while (active) {
            processNextResponse();
        }
    }


    public void term() {
        active = false;
        try { socket.close(); }
        catch( Exception err ) { ; }
    }

    public void popResponse() {
        if (len(cached_results) == 0) return null;
        else return cached_results.pop();
    }

    public String getMessageField( String header, int index) {
        String[] toks = header.split("[|]");
        return toks[index];
    }

    public void postArray( String header, byte[] data ) { ; }


    public void processNextResponse() {
        try {
            String response = new String(socket.recv(0)).trim();
            String[] toks = response.split("[!]");
            String rId = toks[0];
            String type = toks[1];
            if ( type == "array" ) {
                String header = toks[2];
                byte[] data = socket.recv();
                postArray(header, data);

            } else if ( type =="response" ) {
                cacheResult(rId, toks[2]);
                logger.info("Received result: {0}".format(toks[2]));
            } else {
                logger.error("CDASPortal.ResponseThread-> Received unrecognized message type: {0}".format(type));
            }

        } catch( Exception err ) {
            logger.error("CDAS error: {0}\n{1}\n".format(err, traceback.format_exc()));
        }
    }

    public void getResponses( String rId, Boolean wait ) {
        while (true) {
            results = getResults(rId);
            if (( results.size() > 0 ) || !wait) { return results; }
            else { try{ sleep(250 ); } catch(Exception err) { ; } }
        }
    }
}

public class CDASPortalClient {
    protected int MB = 1024 * 1024;
    protected Logger logger = CDASLogManager.getCurrentLogger();
    protected ZMQ.Context zmqContext = null;
    protected ZMQ.Socket request_socket = null;
    protected ZMQ.Socket response_socket = null;
    protected String app_host = null;
    protected ResponseManager response_manager = null;

    // =ConnectionMode.BIND ="localhost"=0=0
    public void __init__(int connectionMode , String host, int request_port, int response_port ) {
        try {
            zmqContext = ZMQ.context(1);
            request_socket = zmqContext.socket(ZMQ.PUSH);
            response_socket = zmqContext.socket(ZMQ.PULL);
            app_host = host;
            if (connectionMode == ConnectionMode.BIND) {
                request_port = ConnectionMode.bindSocket(request_socket, request_port);
                response_port = ConnectionMode.bindSocket(response_socket, response_port);
                logger.info(String.format("Binding request socket to port: %d",request_port));
                logger.info(String.format("Binding response socket to port: %d",response_port));
            } else {
                request_port = ConnectionMode.connectSocket(request_socket, app_host, request_port);
                response_port = ConnectionMode.connectSocket(response_socket, app_host, response_port);
                logger.info(String.format("Connected request socket to server %s on port: %d",app_host, request_port));
                logger.info(String.format("Connected response socket on port: %d",response_port));
            }


        } catch(Exception err) {
            String err_msg = "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc());
            logger.error(err_msg);
            println( err_msg
            shutdown();
        }
    }

    public void del() {
        shutdown();
    }

    public void createResponseManager() {
        response_manager = new ResponseManager()
        response_manager.start()
        return response_manager
    }

    public void shutdown() {
        logger.info(" ############################## SHUT DOWN CDAS PORTAL ##############################")
        try { request_socket.close(); }
        catch ( Exception err ) {;}
        if( response_manager != null ) {
            response_manager.term();
        }
    }

    public void randomId(length) {
        sample = string.lowercase + string.digits + string.uppercase
        return ''.join(random.choice(sample) for i in range(length))
    }

    public void sendMessage(type, mDataList =[""]) {
        msgId = randomId(8)
        msgStrs = [str(mData).replace("'", '"') for mData in mDataList ]
        logger.info("Sending {0} request {1} on port {2}.".format(type, msgStrs, request_port))
        try:
        message = "!".join([msgId, type]+msgStrs )
        request_socket.send(message)
        except zmq.error.ZMQError as err:
        logger.error("Error sending message {0} on request socket: {1}".format(message, str(err)))
        return msgId
    }
}






