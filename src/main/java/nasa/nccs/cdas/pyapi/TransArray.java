package nasa.nccs.cdas.pyapi;

public class TransArray {
    public int[] shape;
    public int[] origin;
    public int nbytes;
    public float invalid;
    public String metadata;

    public TransArray( String init_metadata, int[] init_shape, int[] init_origin, float init_invalid ) {
        shape = init_shape;
        origin = init_origin;
        nbytes = getNBytes();
        invalid = init_invalid;
        metadata = init_metadata;
    }

    private int getNBytes() {
        int prod = 4;
        for( int i=0; i<shape.length; i++ ) prod *= shape[i];
        return prod;
    }

}
