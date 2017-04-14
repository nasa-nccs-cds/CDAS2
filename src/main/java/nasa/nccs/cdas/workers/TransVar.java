package nasa.nccs.cdas.workers;

import nasa.nccs.cdapi.cdm.NetcdfDatasetMgr;
import ucar.nc2.Attribute;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TransVar {
    String _header;
    String _id;
    String _nodeId;
    byte[] _data;
    int[] _origin = null;
    int[] _shape = null;
    Map<String, String> _metadata;

    public TransVar( String header, byte[] data ) {
        _header = header;
        _data = data;
        String[] header_items = header.split("[|]");
        _nodeId = header_items[0].split("[-]")[1];
        _id = header_items[1];
        _origin = s2ia( header_items[2] );
        _shape = s2ia( header_items[3] );
        _metadata = s2m( header_items[4] );
    }

    public String toString() {
        return String.format("TransVar: id=%s, header=%s", _id, _header );
    }

    public int[] getOrigin() { return _origin; }
    public int[] getShape() { return _shape; }
    public byte[] getData() { return _data; }
    public String id() { return _id; }
    public ByteBuffer getDataBuffer() { return ByteBuffer.wrap(_data); }
    public Map<String, String> getMetaData() { return _metadata; }

    public float getInvalid() throws IOException {
        String gridfile = _metadata.get("gridfile");
        String name = _metadata.get("name");
        NetcdfDataset ncd = NetcdfDatasetMgr.open(gridfile);
        Variable var = ncd.findVariable(null,name);
        Attribute missing = var.findAttribute("missing_value");
        return missing.getNumericValue().floatValue();
    }

    private int[] s2ia( String s ) {
        String[] items = s.split("[,]");
        int[] results = new int[items.length];
        for (int i = 0; i < items.length; i++) {
            try {
                results[i] = Integer.parseInt(items[i]);
            } catch (NumberFormatException nfe) { results[i] = Integer.MAX_VALUE; };
        }
        return results;
    }

    private Map<String, String>  s2m( String s ) {
        String[] items = s.split("[;]");
        Map<String, String>  results = new HashMap();
        for (int i = 0; i < items.length; i++) {
            String[] subitems = items[i].split("[:]");
            try{ results.put( subitems[0], subitems[1] ); } catch( ArrayIndexOutOfBoundsException err ) {;}
        }
        return results;
    }
}
