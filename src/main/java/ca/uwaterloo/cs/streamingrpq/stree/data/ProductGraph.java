package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import javax.management.Query;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class ProductGraph<V,L> {
    private Multimap<ProductGraphNode<V>, GraphEdge<ProductGraphNode<V>>> forwardAdjacency;
    private Multimap<ProductGraphNode<V>, GraphEdge<ProductGraphNode<V>>> backwardAdjacency;
    private QueryAutomata<L> automata;

    private Table<V, Integer, ProductGraphNode<V>> nodeIndex;

    private int edgeCount;

    protected Counter edgeCounter;

    private LinkedList<GraphEdge<ProductGraphNode<V>>> timeOrderedEdges;

    public ProductGraph(int capacity, QueryAutomata<L> automata) {
        forwardAdjacency = HashMultimap.create(capacity, Constants.EXPECTED_NEIGHBOURS);
        backwardAdjacency = HashMultimap.create(capacity, Constants.EXPECTED_NEIGHBOURS);
        timeOrderedEdges = new LinkedList<GraphEdge<ProductGraphNode<V>>>();
        nodeIndex = HashBasedTable.create(capacity, Constants.EXPECTED_NEIGHBOURS);
        this.automata = automata;
        this.edgeCount = 0;
    }

    public void addEdge(V source, V target, L label, long timestamp) {
        Map<Integer, Integer> transitions = automata.getTransition(label);
        for(Map.Entry<Integer, Integer> transition : transitions.entrySet()) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();
            ProductGraphNode<V> sourceNode = this.getNode(source, sourceState);
            ProductGraphNode<V> targetNode = this.getNode(target, targetState);
            GraphEdge<ProductGraphNode<V>> forwardEdge = new GraphEdge<>(sourceNode, targetNode, timestamp);
            forwardAdjacency.put(sourceNode, forwardEdge);
            backwardAdjacency.put(targetNode, forwardEdge);
            timeOrderedEdges.add(forwardEdge);
        }

        edgeCounter.inc();
        edgeCount++;
    }

    private ProductGraphNode<V> getNode(V vertex, int state) {
        ProductGraphNode<V> node = this.nodeIndex.get(vertex, state);
        if(node == null) {
            node = new ProductGraphNode<>(vertex, state);
            this.nodeIndex.put(vertex, state, node);
        }
        return node;
    }

    private void removeEdgeFromHashIndexes(GraphEdge<ProductGraphNode<V>> edge) {
        forwardAdjacency.remove(edge.getSource(), edge);
        backwardAdjacency.remove(edge.getTarget(), edge);
    }

    public Collection<ProductGraphNode<V>> getVertices() {
        return forwardAdjacency.keySet();
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getForwardEdges(V source, int state) {
        return getForwardEdges(this.getNode(source, state));
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getForwardEdges(ProductGraphNode<V> node) {
        return forwardAdjacency.get(node);
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getBackwardEdges(V source, int state) {
        return backwardAdjacency.get(getNode(source, state));
    }

    /**
     * removes old edges from the productGraph, used during window management
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     */
    public void removeOldEdges(long minTimestamp) {
        // it suffices to linearly scan from the oldest edge as we assume ordered arrival
        Iterator<GraphEdge<ProductGraphNode<V>>> edgeIterator = timeOrderedEdges.iterator();
        while(edgeIterator.hasNext()) {
            GraphEdge<ProductGraphNode<V>> oldestEdge = edgeIterator.next();
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
