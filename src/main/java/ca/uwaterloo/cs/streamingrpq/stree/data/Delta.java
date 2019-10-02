package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.Multimap;

import java.util.Collection;
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

    public Collection<SpanningTree> getTrees() {
        return treeIndex.values();
    }

    public boolean exists(V vertex) {
        return treeIndex.containsKey(vertex);
    }

    public void addTree(V vertex) {
        if(exists(vertex)) {
            // TODO one spanner per root vertex
        }
        SpanningTree<V> tree = new SpanningTree<>(this, vertex);
        treeIndex.put(vertex, tree);
    }
}
