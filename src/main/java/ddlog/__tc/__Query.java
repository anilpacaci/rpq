// automatically generated by the FlatBuffers compiler, do not modify

package ddlog.__tc;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class __Query extends Table {
  public static __Query getRootAs__Query(ByteBuffer _bb) { return getRootAs__Query(_bb, new __Query()); }
  public static __Query getRootAs__Query(ByteBuffer _bb, __Query obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public __Query __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long idxid() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public byte keyType() { int o = __offset(6); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public Table key(Table obj) { int o = __offset(8); return o != 0 ? __union(obj, o) : null; }

  public static int create__Query(FlatBufferBuilder builder,
      long idxid,
      byte key_type,
      int keyOffset) {
    builder.startObject(3);
    __Query.addIdxid(builder, idxid);
    __Query.addKey(builder, keyOffset);
    __Query.addKeyType(builder, key_type);
    return __Query.end__Query(builder);
  }

  public static void start__Query(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addIdxid(FlatBufferBuilder builder, long idxid) { builder.addLong(0, idxid, 0L); }
  public static void addKeyType(FlatBufferBuilder builder, byte keyType) { builder.addByte(1, keyType, 0); }
  public static void addKey(FlatBufferBuilder builder, int keyOffset) { builder.addOffset(2, keyOffset, 0); }
  public static int end__Query(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

