package ca.uwaterloo.cs.streamingrpq.stree.util;

import java.util.Map;

public class Hasher {

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

    public static <V> MapKey<V> getTreeNodePairKey(V vertex, int state) {
        return new MapKey<>(vertex, state);
    }

    public static class MapKey<V> {

        public final V X;
        public final int Y;

        public MapKey(final V X, final int Y) {
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
