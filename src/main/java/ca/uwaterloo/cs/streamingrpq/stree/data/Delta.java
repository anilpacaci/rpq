package ca.uwaterloo.cs.streamingrpq.stree.data;

import java.util.HashMap;

public class Delta<V> {

    private HashMap<V, SpanningTree> treeIndex;

    public Delta() {
        treeIndex = new HashMap<>();
    }

    public SpanningTree getTree(V vertex) {
        SpanningTree tree = treeIndex.get(vertex);
        return tree;
    }

    public boolean exists(V vertex) {
        return treeIndex.containsKey(vertex);
    }
}
