package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;


import java.util.HashMap;

public class Graph<V,L> {
    private HashMap<V, Multimap> forwardAdjacency;
    private HashMap<V, Multimap<L, V>> backwardAdjacency;


    public Graph() {
        forwardAdjacency = new HashMap<>();
        backwardAdjacency = new HashMap<>();
    }

    public void addEdge(V source, V target, L label) {
        if(!forwardAdjacency.containsKey(source)) {
            forwardAdjacency.put(source, HashMultimap.create());
        }

        Multimap forwardLabels = forwardAdjacency.get(source);
        forwardLabels.put(label, target);

        if(!backwardAdjacency.containsKey(target)) {
            backwardAdjacency.put(target, HashMultimap.create());
        }

        Multimap backwardLabels = backwardAdjacency.get(target);
        backwardLabels.put(label, source);
    }

    public Multimap<L, V> getForwardEdges(V source) {
        if(forwardAdjacency.containsKey(source)) {
            return forwardAdjacency.get(source);
        }

        // TODO edge does not exist
        return null;
    }

    public Multimap<L, V> getBackwardEdges(V source) {
        if(backwardAdjacency.containsKey(source)) {
            return backwardAdjacency.get(source);
        }

        // TODO edge does not exist
        return null;
    }
}
