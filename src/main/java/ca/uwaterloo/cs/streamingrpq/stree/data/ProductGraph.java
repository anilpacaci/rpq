package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Query;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class ProductGraph<V,L> {

    private QueryAutomata<L> automata;

    private Map<Hasher.MapKey<V>, ProductGraphNode<V>> nodeIndex;

    private int edgeCount;

    protected Counter edgeCounter;

    private LinkedList<GraphEdge<ProductGraphNode<V>>> timeOrderedEdges;

    private final Logger LOG = LoggerFactory.getLogger(ProductGraph.class);

    public ProductGraph(int capacity, QueryAutomata<L> automata) {
        timeOrderedEdges = new LinkedList<GraphEdge<ProductGraphNode<V>>>();
        nodeIndex = Maps.newHashMapWithExpectedSize(capacity);
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
            sourceNode.addForwardEdge(forwardEdge);
            targetNode.addBackwardEdge(forwardEdge);
            timeOrderedEdges.add(forwardEdge);
            edgeCounter.inc();
            edgeCount++;
        }
    }

    private ProductGraphNode<V> getNode(V vertex, int state) {
        ProductGraphNode<V> node = this.nodeIndex.get(Hasher.getTreeNodePairKey(vertex, state));
        if(node == null) {
            node = new ProductGraphNode<>(vertex, state);
            this.nodeIndex.put(Hasher.getTreeNodePairKey(vertex, state), node);
        }
        return node;
    }

    private void removeEdgeFromHashIndexes(GraphEdge<ProductGraphNode<V>> edge) {
        edge.getSource().removeForwardEdge(edge);
        edge.getTarget().removeBackwardEdge(edge);

    }

    public Collection<ProductGraphNode<V>> getVertices() {
        return nodeIndex.values();
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getForwardEdges(V source, int state) {
        return getForwardEdges(this.getNode(source, state));
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getForwardEdges(ProductGraphNode<V> node) {
        return node.getForwardEdges();
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getBackwardEdges(V source, int state) {
        return getBackwardEdges(this.getNode(source, state));
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getBackwardEdges(ProductGraphNode<V> node) {
        return node.getBackwardEdges();
    }

    /**
     * removes old edges from the productGraph, used during window management
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     */
    public void removeOldEdges(long minTimestamp) {
        LOG.info("Batch expiry at {}", minTimestamp);
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

    public int getEdgeCount() {
        return edgeCount;
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.edgeCounter = metricRegistry.counter("edge-counter");
    }
}
