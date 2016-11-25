package nasa.nccs.cdas.pyapi;
import org.zeromq.ZMQ;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;


public class PythonWorkerManager {
    ZMQ.Context zmqContext = null;
    int worker_index = 0;
    ConcurrentLinkedQueue<PythonWorker> availableWorkers = null;
    ConcurrentLinkedQueue<PythonWorker> busyWorkers = null;

    private PythonWorkerManager(){
        zmqContext = ZMQ.context(1);
        availableWorkers = new ConcurrentLinkedQueue<PythonWorker>();
        busyWorkers = new ConcurrentLinkedQueue<PythonWorker>();
    }

    private static class SingletonHelper{
        private static final PythonWorkerManager INSTANCE = new PythonWorkerManager();
    }

    public static PythonWorkerManager getInstance(){
        return SingletonHelper.INSTANCE;
    }

    public PythonWorker getWorker() {
        PythonWorker worker = availableWorkers.poll();
        if( worker == null ) {
            worker = new PythonWorker( worker_index, zmqContext );
            worker_index += 1;
        }
        busyWorkers.add( worker );
        return worker;
    }

    public void releaseWorker( PythonWorker worker ) {
        busyWorkers.remove( worker );
        availableWorkers.add( worker );
    }

    public void shutdown() {
        System.out.println( "\t   *** PythonWorkerManager SHUTDOWN *** " );
        while( !availableWorkers.isEmpty() ) { availableWorkers.poll().shutdown(); }
        while( !busyWorkers.isEmpty() ) { busyWorkers.poll().shutdown(); }
        zmqContext.term();
        printPythonLog(0);
    }

    private void printPythonLog( int iPartition ) {
        try {
            Path path = FileSystems.getDefault().getPath(System.getProperty("user.home"), ".cdas", String.format("pycdas-%d.log", iPartition));
            BufferedReader br = new BufferedReader(new FileReader(path.toString()));
            System.out.println( "\tPYTHON LOG: PARTITION-" + String.valueOf(iPartition) );
            String line = br.readLine();
            while (line != null) {
                System.out.println( line );
                line = br.readLine();
            }
            br.close();
        } catch ( IOException ex ) {
            System.out.println( "Error reading log file : " + ex.toString() );
        }
    }
}
