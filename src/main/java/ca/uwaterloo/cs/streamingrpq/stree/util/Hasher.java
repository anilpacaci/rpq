package ca.uwaterloo.cs.streamingrpq.stree.util;

public class Hasher {

    public static int TreeNodeHasher(int vertex, int state) {
        int h = 17;
        h = 31 * h + vertex;
        h = 31 * h + state;
        return h;
    }
}