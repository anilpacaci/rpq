package ca.uwaterloo.cs.streamingrpq.data;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashMultimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class GraphEdges<N> {

    private HashMultimap<N,N> nodes;

    private Counter counter;

    private Meter edgeCounter;

    public GraphEdges(int capacity, int expectedKeys) {
        nodes = HashMultimap.create(capacity, expectedKeys);
        counter = new Counter();
    }

    public void addNeighbour(N source, N target) {

        nodes.put(source, target);
        counter.inc();
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

