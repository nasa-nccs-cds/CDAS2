package nasa.nccs.cdas.workers.python;
import nasa.nccs.cdas.workers.Worker;
import nasa.nccs.cdas.workers.WorkerPortal;
import java.util.List;
import java.util.ArrayList;

public class PythonWorkerPortal extends WorkerPortal {
    private List<PythonWorker> workers;

    private PythonWorkerPortal(){
        super();
        workers = new ArrayList<PythonWorker>();
    }

    public String[] getCapabilities() {
        try {
            PythonWorker worker = (workers.size() > 0) ? workers.get(0) : getPythonWorker();
            return worker.getCapabilities().split("[|]");
        } catch ( Exception ex ) {
            return null;
        }
    }

    private static class SingletonHelper{
        private static final PythonWorkerPortal INSTANCE = new PythonWorkerPortal();
    }

    public static PythonWorkerPortal getInstance(){
        return PythonWorkerPortal.SingletonHelper.INSTANCE;
    }

    protected Worker newWorker() throws Exception {
        PythonWorker worker = new PythonWorker( zmqContext, logger );
        this.workers.add( worker );
        return worker;
    }

    public PythonWorker getPythonWorker() throws Exception { return (PythonWorker) getWorker(); }

    public void quit() {
        for( int i=0; i<this.workers.size(); i++ ) { this.workers.remove(i).quit(); }
    }
}
