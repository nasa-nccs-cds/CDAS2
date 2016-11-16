package nasa.nccs.cdas.pyapi;

public class TransArray {
    public int[] shape;
    public int[] origin;
    public float[] data;
    public float invalid;
    public String metadata;

    public TransArray( String init_metadata, int[] init_shape, int[] init_origin, float[] init_data, float init_invalid ) {
        shape = init_shape;
        origin = init_origin;
        data = init_data;
        invalid = init_invalid;
        metadata = init_metadata;
    }
}
