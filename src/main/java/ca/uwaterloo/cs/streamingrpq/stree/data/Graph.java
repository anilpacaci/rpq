package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class Graph<V,L> {
    private Multimap<V, GraphEdge<V,L>> forwardAdjacency;
    private Multimap<V, GraphEdge<V,L>> backwardAdjacency;

    private int edgeCount;

    protected Counter edgeCounter;

    private LinkedList<GraphEdge<V,L>> timeOrderedEdges;

    public Graph(int capacity) {
        forwardAdjacency = HashMultimap.create(capacity, Constants.EXPECTED_NEIGHBOURS);
        backwardAdjacency = HashMultimap.create(capacity, Constants.EXPECTED_NEIGHBOURS);
        timeOrderedEdges = new LinkedList<GraphEdge<V,L>>();
        int edgeCount = 0;
    }

    public void addEdge(V source, V target, L label, long timestamp) {
        GraphEdge<V, L> forwardEdge = new GraphEdge<>(source, target, label, timestamp);
        forwardAdjacency.put(source, forwardEdge);
        backwardAdjacency.put(target, forwardEdge);

        timeOrderedEdges.addLast(forwardEdge);
        edgeCounter.inc();
        edgeCount++;
    }

    private void removeEdgeFromHashIndexes(V source, V target, L label) {
        GraphEdge<V, L> forwardEdge = new GraphEdge<>(source, target, label, 0);
        forwardAdjacency.remove(source, forwardEdge);
        backwardAdjacency.remove(target, new GraphEdge<>(source, target, label, 0));
    }

    private void removeEdgeFromHashIndexes(GraphEdge<V, L> edge) {
        forwardAdjacency.remove(edge.getSource(), edge);
        backwardAdjacency.remove(edge.getTarget(), edge);
    }

    public Collection<GraphEdge<V, L>> getForwardEdges(V source) {
        return forwardAdjacency.get(source);
    }

    public Collection<GraphEdge<V, L>> getBackwardEdges(V target) {
        return backwardAdjacency.get(target);
    }

    /**
     * removes old edges from the graph, used during window management
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     */
    public void removeOldEdges(long minTimestamp) {
        // it suffices to linearly scan from the oldest edge as we assume ordered arrival
        Iterator<GraphEdge<V, L>> edgeIterator = timeOrderedEdges.iterator();
        while(edgeIterator.hasNext()) {
            GraphEdge<V, L> oldestEdge = edgeIterator.next();
            if(oldestEdge.getTimestamp() <= minTimestamp) {
                edgeIterator.remove();
                removeEdgeFromHashIndexes(oldestEdge);
                edgeCounter.dec();
                edgeCount--;
            } else {
                // as we assume ordered arrival, we can stop the search
                break;
            }
        }
    }

    protected int getEdgeCount() {
        return edgeCount;
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.edgeCounter = metricRegistry.counter("edge-counter");
    }
}
