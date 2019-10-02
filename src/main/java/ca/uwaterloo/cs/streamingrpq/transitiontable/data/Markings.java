package ca.uwaterloo.cs.streamingrpq.transitiontable.data;

import com.google.common.collect.HashMultimap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Markings<N, P, T> {

    private Map<N, HashMultimap<P, T>> sourceToMarkings;

    public Markings(int capacity, int expectedKeys) {
        sourceToMarkings = new HashMap<>();
    }

    public boolean contains(N source, P node) {
        HashMultimap<P, T> markings = sourceToMarkings.get(source);
        if(markings == null) {
            return false;
        }
        return markings.containsKey(node);
    }

    public Collection<T> getCrossEdges(N source, P node) {
        HashMultimap<P, T> markings = sourceToMarkings.get(source);
        if(markings == null) {
            return Collections.EMPTY_LIST;
        }
        return markings.get(node);
    }

    public void addMarking(N source, P node) {
        HashMultimap<P, T> markings = sourceToMarkings.get(source);
        if(markings == null) {
            markings = HashMultimap.create();
        }
        markings.put(node, null);
    }

    public void addCrossEdge(N source, P node, T crossEdge) {
        HashMultimap<P, T> markings = sourceToMarkings.get(source);
        if(markings == null) {
            markings = HashMultimap.create();
        }
        markings.put(node, crossEdge);
    }

    public void removeMarking(N source, P node) {
        HashMultimap<P, T> markings = sourceToMarkings.get(source);
        if(markings == null) {
            return;
        }
        markings.removeAll(node);
    }

    public int getNodeCount() {
        return sourceToMarkings.values().size();
    }

    public int getEdgeCount() {
        return sourceToMarkings.values().stream().mapToInt(v -> v.values().size()).sum();
    }
}
