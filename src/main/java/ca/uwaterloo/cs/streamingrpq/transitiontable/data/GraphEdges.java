package ca.uwaterloo.cs.streamingrpq.transitiontable.data;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashMultimap;

import java.util.Collection;

public class GraphEdges<N> {

    private HashMultimap<N,N> nodes;

    private Meter edgeCounter;

    public GraphEdges(int capacity, int expectedKeys) {
        nodes = HashMultimap.create(capacity, expectedKeys);
    }

    public void addNeighbour(N source, N target) {

        nodes.put(source, target);
        edgeCounter.mark();
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

    public void setMetricRegistry(MetricRegistry registry) {
        edgeCounter = registry.meter("edge-counter");
        edgeCounter.mark();
    }

}

