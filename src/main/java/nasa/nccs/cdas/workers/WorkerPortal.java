package nasa.nccs.cdas.workers;
import nasa.nccs.cdas.workers.Worker;
import nasa.nccs.utilities.CDASLogManager;
import org.zeromq.ZMQ;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class WorkerPortal {
    protected ZMQ.Context zmqContext = null;
    protected ConcurrentLinkedQueue<Worker> availableWorkers = null;
    protected ConcurrentLinkedQueue<Worker> busyWorkers = null;
    protected Logger logger = CDASLogManager.getCurrentLogger();

    protected WorkerPortal(){
        zmqContext = ZMQ.context(1);
        availableWorkers = new ConcurrentLinkedQueue<Worker>();
        busyWorkers = new ConcurrentLinkedQueue<Worker>();
    }

    public Worker getWorker() {
        Worker worker = availableWorkers.poll();
        if( worker == null ) { worker =  newWorker(); }
        busyWorkers.add( worker );
        return worker;
    }

    protected abstract Worker newWorker();

    public void releaseWorker( Worker worker ) {
        busyWorkers.remove( worker );
        availableWorkers.add( worker );
    }

    int getNumWorkers() { return availableWorkers.size() + busyWorkers.size(); }

    public void shutdown() {
        logger.info( "\t   *** WorkerPortal SHUTDOWN *** " );
        while( !availableWorkers.isEmpty() ) { availableWorkers.poll().shutdown(); }
        while( !busyWorkers.isEmpty() ) { busyWorkers.poll().shutdown(); }
        try { Thread.sleep(2000); } catch ( Exception ex ) {;}
        printPythonLog(0);
    }

    private void printPythonLog( int iPartition ) {
        try {
            Path path = FileSystems.getDefault().getPath(System.getProperty("user.home"), ".cdas", String.format("pycdas-%d.log", iPartition));
            BufferedReader br = new BufferedReader(new FileReader(path.toString()));
            logger.info( "\tPYTHON LOG: PARTITION-" + String.valueOf(iPartition) );
            String line = br.readLine();
            while (line != null) {
                System.out.println( line );
                line = br.readLine();
            }
            br.close();
        } catch ( IOException ex ) {
            logger.info( "Error reading log file : " + ex.toString() );
        }
    }
}
