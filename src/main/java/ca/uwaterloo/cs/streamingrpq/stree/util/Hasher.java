package ca.uwaterloo.cs.streamingrpq.stree.util;

import java.util.Map;

public class Hasher {

    private static ThreadLocal<MapKey> threadLocalKey = new ThreadLocal<>();

    public static int TreeNodeHasher(int vertex, int state) {
        int h = 17;
        h = 31 * h + vertex;
        h = 31 * h + state;
        return h;
    }

    public static <V> int TreeNodeHasher(V vertex, int state) {
        int h = 17;
        h = 31 * h + vertex.hashCode();
        h = 31 * h + state;
        return h;
    }

    public static <V> MapKey<V> createTreeNodePairKey(V vertex, int state) {
        MapKey<V> mapKey = new MapKey<V>(vertex, state);
        return mapKey;
    }

    public static <V> MapKey<V> getThreadLocalTreeNodePairKey(V vertex, int state) {

        MapKey<V> mapKey = threadLocalKey.get();
        if(mapKey == null) {
         mapKey = new MapKey<V>(vertex, state);
         threadLocalKey.set(mapKey);
        } else {
            mapKey.X = vertex;
            mapKey.Y = state;
        }
        return mapKey;
    }

    public static class MapKey<V> {

        public V X;
        public int Y;

        public MapKey(V X, int Y) {
            this.X = X;
            this.Y = Y;
        }

        @Override
        public boolean equals (final Object O) {
            if (!(O instanceof MapKey)) return false;
            if (!((MapKey) O).X.equals(X)) return false;
            if (((MapKey) O).Y != Y) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 31 * h + X.hashCode();
            h = 31 * h + Y;
            return h;
        }

    }
}
