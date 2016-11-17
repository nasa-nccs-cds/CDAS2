package nasa.nccs.cdas.pyapi;
import nasa.nccs.cdas.pyapi.TransArray;

import java.util.List;


public interface ICDAS {

    public String sayHello(int i, String s);

    public String execute( String op, String context, List<TransArray> data );
}
