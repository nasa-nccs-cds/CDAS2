package nasa.nccs.cdas.pyapi;

public class TransData {
    public int[] shape;
    public float[] data;

    public TransData( int[] init_shape, float[] init_data ) {
        shape = init_shape;
        data = init_data;
    }
    public int[] getShape() { return shape; }
    public float[] getData() { return data; }
}
