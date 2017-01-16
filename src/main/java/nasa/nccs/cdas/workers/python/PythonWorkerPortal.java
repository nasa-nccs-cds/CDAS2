package nasa.nccs.cdas.workers.python;
import nasa.nccs.cdas.workers.Worker;
import nasa.nccs.cdas.workers.WorkerPortal;

public class PythonWorkerPortal extends WorkerPortal {

    private PythonWorkerPortal() {
        super();
    }

    public String[] getCapabilities() {
        try {
            PythonWorker worker = getPythonWorker();
            String response =  worker.getCapabilities();
            logger.info( "GET CAPABILITIES RESPONSE: " + response );
            releaseWorker(worker);
            return response.split("[|]");
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

    protected Worker newWorker() throws Exception { return  new PythonWorker( zmqContext, logger ); }

    public PythonWorker getPythonWorker() throws Exception { return (PythonWorker) getWorker(); }

    public void quit() { shutdown(); }
}
