package nasa.nccs.cdas.pyapi;


public class TransVar {
    String _header;
    byte[] _data;

    public TransVar( String header, byte[] data ) {
        _header = header;
        _data = data;
    }
}
