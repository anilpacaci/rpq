// Automatically generated by the DDlog compiler.
package ddlog.tc;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import com.google.flatbuffers.*;
public class tcQuery
{
    public static void query__Null_by_none(DDlogAPI hddlog, java.util.function.Consumer<Tuple0Reader> callback)throws DDlogException
    {
        FlatBufferBuilder fbbuilder = new FlatBufferBuilder();
        int query = ddlog.__tc.__Query.create__Query(fbbuilder, 0, ddlog.__tc.__Value.Tuple0, ((java.util.function.Supplier<Integer>) (() -> 
                                                                                              {
                                                                                                  ddlog.__tc.Tuple0.startTuple0(fbbuilder);
                                                                                                  return Integer.valueOf( ddlog.__tc.Tuple0.endTuple0(fbbuilder));
                                                                                              })).get());
        fbbuilder.finish(query);
        DDlogAPI.FlatBufDescr resfb = new DDlogAPI.FlatBufDescr();
        hddlog.queryIndexFromFlatBuf(fbbuilder.dataBuffer(), resfb);
        try {
            ddlog.__tc.__Values vals = ddlog.__tc.__Values.getRootAs__Values(resfb.buf);
            int len = vals.valuesLength();
            for (int i = 0; i < len; i++) {
                ddlog.__tc.Tuple0 __val = (ddlog.__tc.Tuple0)vals.values(i).v(new ddlog.__tc.Tuple0());
                callback.accept(new Tuple0Reader(__val));
            }
        } finally { hddlog.flatbufFree(resfb); }
    }
    public static void dump__Null_by_none(DDlogAPI hddlog, java.util.function.Consumer<Tuple0Reader> callback)throws DDlogException
    {
        DDlogAPI.FlatBufDescr resfb = new DDlogAPI.FlatBufDescr();
        hddlog.dumpIndexToFlatBuf(0, resfb);
        try {
            ddlog.__tc.__Values vals = ddlog.__tc.__Values.getRootAs__Values(resfb.buf);
            int len = vals.valuesLength();
            for (int i = 0; i < len; i++) {
                ddlog.__tc.Tuple0 __val = (ddlog.__tc.Tuple0)vals.values(i).v(new ddlog.__tc.Tuple0());
                callback.accept(new Tuple0Reader(__val));
            }
        } finally { hddlog.flatbufFree(resfb); }
    }
}