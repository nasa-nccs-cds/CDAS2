package nasa.nccs.cdas.workers.python;
import nasa.nccs.cdas.workers.Worker;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.Logger;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

public class PythonWorker extends Worker {
    Process proc;

    public PythonWorker( ZMQ.Context context, Logger logger ) throws Exception {
        super( context, logger );
        proc = startup();
        logger.info( " *** Started worker process: " +  proc.toString() );
    }

    Process startup() throws Exception {
        try {
            FileSystem fileSystems = FileSystems.getDefault();
            Path log_path = fileSystems.getPath( System.getProperty("user.home"), ".cdas", "logs", String.format("python-worker-%d.log",request_port) );
            Path run_script = fileSystems.getPath( System.getProperty("user.home"), ".cdas", "sbin", "startup_python_worker.sh" );
            Map<String, String> sysenv = System.getenv();
//            ProcessBuilder pb = new ProcessBuilder( "python", "-m", "pycdas.worker", String.valueOf(request_port), String.valueOf(result_port) );
            ProcessBuilder pb = new ProcessBuilder( run_script.toString(), String.valueOf(request_port), String.valueOf(result_port) );
            Map<String, String> env = pb.environment();
            for (Map.Entry<String, String> entry : sysenv.entrySet()) { env.put( entry.getKey(), entry.getValue() ); }
            pb.redirectErrorStream( true );
            pb.redirectOutput( ProcessBuilder.Redirect.appendTo( log_path.toFile() ));
            logger.info( " *** Starting Python Worker: pycdas.worker.Worker --> request_port = " + String.valueOf(request_port)+ ", result_port = " + String.valueOf(result_port));
            return pb.start();
        } catch ( IOException ex ) {
            throw new Exception( "Error starting Python Worker : " + ex.toString() );
        }
    }

}
