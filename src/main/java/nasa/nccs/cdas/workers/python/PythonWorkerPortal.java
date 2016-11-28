package nasa.nccs.cdas.workers.python;
import nasa.nccs.cdas.workers.Worker;
import nasa.nccs.cdas.workers.WorkerPortal;

public class PythonWorkerPortal extends WorkerPortal {

    private PythonWorkerPortal(){ super(); }

    private static class SingletonHelper{
        private static final PythonWorkerPortal INSTANCE = new PythonWorkerPortal();
    }

    public static PythonWorkerPortal getInstance(){
        return PythonWorkerPortal.SingletonHelper.INSTANCE;
    }

    protected Worker newWorker() { return new PythonWorker( zmqContext, logger ); }

    public PythonWorker getPythonWorker() { return (PythonWorker) getWorker(); }

}
