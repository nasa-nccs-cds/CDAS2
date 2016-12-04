package nasa.nccs.cdas.workers.python;
import nasa.nccs.cdas.workers.Worker;
import org.zeromq.ZMQ;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

public class PythonWorker extends Worker {

    public PythonWorker( ZMQ.Context context, Logger logger ) {
        super( context, logger );
        startup();
    }

    Process startup() {
        try {
            String cdas_home = System.getenv("CDAS_HOME_DIR");
            Path path = FileSystems.getDefault().getPath(cdas_home, "bin", "run_worker.sh" );
            Path python_root = FileSystems.getDefault().getPath( cdas_home, "python" );
            Path exe_dir = FileSystems.getDefault().getPath( cdas_home, "bin" );
            Path log_path = FileSystems.getDefault().getPath( System.getProperty("user.home"), ".cdas", String.format("python-worker-%d.log",request_port) );
            ProcessBuilder pb = new ProcessBuilder( path.toString(), String.valueOf(request_port), String.valueOf(result_port) );
            Map<String, String> env = pb.environment();
            env.put("PYTHONPATH", env.get("PYTHONPATH") + ":" + python_root.toString() );
            pb.directory( exe_dir.toFile() );
            pb.redirectErrorStream( true );
            pb.redirectOutput( ProcessBuilder.Redirect.appendTo( log_path.toFile() ));
            System.out.println( "Starting Python Worker: " + String.valueOf(request_port) + " " + String.valueOf(result_port) );
            return pb.start();
        } catch ( IOException ex ) {
            System.out.println( "Error starting Python Worker : " + ex.toString() );
            return null;
        }
    }

}
