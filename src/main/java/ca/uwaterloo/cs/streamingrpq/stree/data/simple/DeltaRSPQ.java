package ca.uwaterloo.cs.streamingrpq.stree.data.simple;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class DeltaRSPQ<V> {

    private Map<V, SpanningTreeRSPQ> treeIndex;
    private Map<Hasher.MapKey<V>, Set<SpanningTreeRSPQ>> nodeToTreeIndex;

    protected Counter treeCounter;
    protected Histogram maintainedTreeHistogram;

    private final Logger LOG = LoggerFactory.getLogger(DeltaRSPQ.class);

    public DeltaRSPQ(int capacity) {
        treeIndex = Maps.newConcurrentMap();
        nodeToTreeIndex = Maps.newConcurrentMap();
    }

    public Collection<SpanningTreeRSPQ> getTrees(V vertex, int state) {
        Set<SpanningTreeRSPQ> containingTrees = nodeToTreeIndex.computeIfAbsent(Hasher.getTreeNodePairKey(vertex, state), key ->
                Collections.newSetFromMap(new ConcurrentHashMap<SpanningTreeRSPQ, Boolean>()) );
        return containingTrees;
    }

    public boolean exists(V vertex) {
        return treeIndex.containsKey(vertex);
    }

    public SpanningTreeRSPQ<V> addTree(V vertex, long timestamp) {
        if(exists(vertex)) {
            // TODO one spanner per root vertex
        }
        SpanningTreeRSPQ<V> tree = new SpanningTreeRSPQ<>(this, vertex, timestamp);
        treeIndex.put(vertex, tree);
        addToTreeNodeIndex(tree, tree.getRootNode());
        treeCounter.inc();
        return tree;
    }

    public void removeTree(SpanningTreeRSPQ<V> tree) {
        TreeNodeRSPQ<V> rootNode = tree.getRootNode();
        this.treeIndex.remove(rootNode.getVertex());

        Collection<SpanningTreeRSPQ> containingTrees = getTrees(rootNode.getVertex(), rootNode.getState());
        containingTrees.remove(tree);
        if(containingTrees.isEmpty()) {
            nodeToTreeIndex.remove(Hasher.getTreeNodePairKey(rootNode.getVertex(), rootNode.getState()));
        }
        treeCounter.dec();
    }

    public void addToTreeNodeIndex(SpanningTreeRSPQ<V> tree, TreeNodeRSPQ<V> treeNode) {
        Collection<SpanningTreeRSPQ> containingTrees = getTrees(treeNode.getVertex(), treeNode.getState());
        containingTrees.add(tree);
    }

    public void removeFromTreeIndex(TreeNodeRSPQ<V> removedNode, SpanningTreeRSPQ<V> tree) {
        Collection<SpanningTreeRSPQ> containingTrees = this.getTrees(removedNode.getVertex(), removedNode.getState());
        containingTrees.remove(tree);
        if(containingTrees.isEmpty()) {
            this.nodeToTreeIndex.remove(Hasher.getTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
        }
    }


    /**
     * Updates Perform window expiry on each spanning tree
     * @param minTimestamp lower bound on the window interval
     * @param productGraph snapshotGraph
     * @param executorService
     * @param <L>
     */
    public <L> void expiry(Long minTimestamp, ProductGraph<V,L> productGraph, ExecutorService executorService) {
        Collection<SpanningTreeRSPQ> trees = treeIndex.values();
        List<Future<Void>> futures = new ArrayList<>(trees.size());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        LOG.info("{} of trees in DeltaRAPQ", trees.size());
        for(SpanningTreeRSPQ<V> tree : trees) {
            if (tree.getMinTimestamp() > minTimestamp) {
                // this tree does not have any node to be deleted, so just skip it
                continue;
            }

            RSPQSpanningTreeExpiryJob<V, L> RSPQSpanningTreeExpiryJob = new RSPQSpanningTreeExpiryJob<V, L>(minTimestamp, productGraph, tree);
            futures.add(completionService.submit(RSPQSpanningTreeExpiryJob));
        }

        for(int i = 0; i < futures.size(); i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("SpanningTreeExpiry interrupted during execution", e);
            }
        }

        LOG.info("Expiry at {}: # of trees {}, # of edges in the productGraph {}", minTimestamp, treeIndex.size(), productGraph.getEdgeCount());
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.treeCounter = metricRegistry.counter("tree-counter");
        this.maintainedTreeHistogram = metricRegistry.histogram("expired-tree-histogram");
    }

    private static class RSPQSpanningTreeExpiryJob<V,L> implements Callable<Void> {

        private Long minTimestamp;
        private ProductGraph<V,L> productGraph;

        private Table<V, Integer, Set<SpanningTreeRAPQ>> nodeToTreeIndex;
        private SpanningTreeRSPQ<V> tree;

        public RSPQSpanningTreeExpiryJob(Long minTimestamp, ProductGraph<V,L> productGraph, SpanningTreeRSPQ<V> tree) {
            this.minTimestamp = minTimestamp;
            this.productGraph = productGraph;
            this.tree = tree;
        }

        @Override
        public Void call() throws Exception {
            tree.removeOldEdges(minTimestamp, productGraph);
            return null;
        }
    }
}
