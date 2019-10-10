package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.*;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Query;
import java.util.*;
import java.util.concurrent.*;

public class ProductGraph<V,L> {

    private QueryAutomata<L> automata;

    private Map<Hasher.MapKey, ProductGraphNode<V>> nodeIndex;

    private int edgeCount;

    protected Counter edgeCounter;

    private final Logger LOG = LoggerFactory.getLogger(ProductGraph.class);

    public ProductGraph(int capacity, QueryAutomata<L> automata) {
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
        }

        edgeCounter.inc();
        edgeCount++;
    }

    private ProductGraphNode<V> getNode(V vertex, int state) {
        ProductGraphNode<V> node = this.nodeIndex.get(Hasher.getTreeNodePairKey(vertex, state));
        if(node == null) {
            node = new ProductGraphNode<>(vertex, state);
            this.nodeIndex.put(Hasher.getTreeNodePairKey(vertex, state), node);
        }
        return node;
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
    public void removeOldEdges(long minTimestamp, ExecutorService executorService) {
        LOG.info("Product Graph expiry at {}", minTimestamp);
        // it suffices to linearly scan from the oldest edge as we assume ordered arrival

        List<Future<ProductGraphNode<V>>> futures = new ArrayList<Future<ProductGraphNode<V>>>(nodeIndex.size());
        CompletionService<ProductGraphNode<V>> completionService = new ExecutorCompletionService<>(executorService);

        // submit each node for expiry
        for(Map.Entry<Hasher.MapKey, ProductGraphNode<V>> entry : nodeIndex.entrySet()) {
            ProductGraphNode<V> node = entry.getValue();
            futures.add(completionService.submit(new ProductGraphNodeExpiryJob<>(node, minTimestamp)));
        }

        // remove nodes that have no edges left after the epxiry job
        for(int i = 0; i < futures.size(); i++) {
            try {
                ProductGraphNode<V> node = completionService.take().get();
                // if node has no edges left, simply remove it from indexes
                if(node.getForwardEdges().isEmpty() && node.getBackwardEdges().isEmpty()) {
                    this.nodeIndex.remove(Hasher.getTreeNodePairKey(node.getVertex(), node.getState()));
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("ProductGraphNodeExpiry interrupted during execution", e);
            }
        }
    }

    protected int getEdgeCount() {
        return edgeCount;
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.edgeCounter = metricRegistry.counter("edge-counter");
    }

    private static class ProductGraphNodeExpiryJob<V> implements Callable<ProductGraphNode<V>> {
        private ProductGraphNode<V> node;
        private long minTimestamp;

        public ProductGraphNodeExpiryJob(ProductGraphNode<V> node, long minTimestamp) {
            this.node = node;
            this.minTimestamp = minTimestamp;
        }

        @Override
        public ProductGraphNode<V> call() throws Exception {
            return node.removeOldEdges(minTimestamp);
        }
    }
}
