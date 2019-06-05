package ca.uwaterloo.cs.streamingrpq.data;

import com.google.common.collect.HashMultimap;

import java.util.Collection;

public class Markings<N, T> {

    private HashMultimap<N, T> markings;

    public Markings(int capacity, int expectedKeys) {
        markings = HashMultimap.create(capacity, expectedKeys);
    }

    public boolean contains(N node) {
        return markings.containsKey(node);
    }

    public Collection<T> getCrossEdges(N node) {
        return markings.get(node);
    }

    public void addMarking(N node) {
        markings.put(node, null);
    }

    public void addCrossEdge(N node, T crossEdge) {
        markings.put(node, crossEdge);
    }

    public void removeMarking(N node) {
        markings.removeAll(node);
    }

    public int getNodeCount() {
        return markings.size();
    }

    public int getEdgeCount() {
        return markings.values().size();
    }}
