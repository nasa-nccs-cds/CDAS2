package nasa.nccs.cdas.pyapi;
import nasa.nccs.cdas.pyapi.TransData;


public interface ICDAS {

    public String sayHello(int i, String s);

    public String sendData( TransData data );

    public String compute( String op, String context, String dataGrid, float[] data, int[] shape );
}
