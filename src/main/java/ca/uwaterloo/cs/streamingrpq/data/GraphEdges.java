package ca.uwaterloo.cs.streamingrpq.data;

import com.google.common.collect.HashMultimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class GraphEdges<N> {

    private HashMultimap<N,N> nodes;

    public GraphEdges(int capacity, int expectedKeys) {
        nodes = HashMultimap.create(capacity, expectedKeys);
    }

    public void addNeighbour(N source, N target) {

        nodes.put(source, target);
    }

    public Collection<N> getNeighbours(N source) {
        return nodes.get(source);
    }

    public boolean contains(N source, N target) {
        return nodes.containsEntry(source, target);
    }

    public int getNodeCount() {
        return nodes.size();
    }

    public int getEdgeCount() {
        return nodes.values().size();
    }

}

