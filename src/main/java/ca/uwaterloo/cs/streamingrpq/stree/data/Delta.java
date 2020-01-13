package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class Delta<V>{

    private Map<V, AbstractSpanningTree<V>> treeIndex;
    private Map<Hasher.MapKey<V>, Set<AbstractSpanningTree<V>>> nodeToTreeIndex;

    protected Counter treeCounter;
    protected Histogram treeSizeHistogram;

    private ObjectFactory<V> objectFactory;

    private final Logger LOG = LoggerFactory.getLogger(Delta.class);

    public Delta(int capacity, ObjectFactory<V> objectFactory) {
        treeIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREES);
        nodeToTreeIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREES);
    }

    public ObjectFactory<V> getObjectFactory() {
        return objectFactory;
    }

    public Collection<AbstractSpanningTree<V>> getTrees(V vertex, int state) {
        Set<AbstractSpanningTree<V>> containingTrees = nodeToTreeIndex.computeIfAbsent(Hasher.createTreeNodePairKey(vertex, state), key -> Collections.newSetFromMap(new ConcurrentHashMap<AbstractSpanningTree<V>, Boolean>()) );
        return containingTrees;
    }

    public boolean exists(V vertex) {
        return treeIndex.containsKey(vertex);
    }



    public AbstractSpanningTree<V> addTree(V vertex, long timestamp) {
        if(exists(vertex)) {
            // TODO one spanner per root vertex
        }
        AbstractSpanningTree<V> tree = objectFactory.createSpanningTree(this, vertex, timestamp);
        treeIndex.put(vertex, tree);
        addToTreeNodeIndex(tree, tree.getRootNode());

        treeCounter.inc();
        return tree;
    }

    public void removeTree(AbstractSpanningTree<V> tree) {
        AbstractTreeNode<V> rootNode = tree.getRootNode();
        this.treeIndex.remove(rootNode.getVertex());

        Collection<AbstractSpanningTree<V>> containingTrees = getTrees(rootNode.getVertex(), rootNode.getState());
        containingTrees.remove(tree);
        if(containingTrees.isEmpty()) {
            nodeToTreeIndex.remove(Hasher.getThreadLocalTreeNodePairKey(rootNode.getVertex(), rootNode.getState()));
        }

        treeCounter.dec();
    }

    public void addToTreeNodeIndex(AbstractSpanningTree<V> tree, AbstractTreeNode<V> treeNode) {
        Collection<AbstractSpanningTree<V>> containingTrees = getTrees(treeNode.getVertex(), treeNode.getState());
        containingTrees.add(tree);
    }

    public void removeFromTreeIndex(AbstractTreeNode<V> removedNode, AbstractSpanningTree<V> tree) {
        Collection<AbstractSpanningTree<V>> containingTrees = this.getTrees(removedNode.getVertex(), removedNode.getState());
        containingTrees.remove(tree);
        if(containingTrees.isEmpty()) {
            this.nodeToTreeIndex.remove(Hasher.getThreadLocalTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
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
        Collection<AbstractSpanningTree<V>> trees = treeIndex.values();
        List<Future<Void>> futures = new ArrayList<>(trees.size());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        LOG.info("{} of trees in Delta", trees.size());
        for(AbstractSpanningTree<V> tree : trees) {
            treeSizeHistogram.update(tree.getSize());
            if (tree.getMinTimestamp() > minTimestamp) {
                // this tree does not have any node to be deleted, so just skip it
                continue;
            }

            RAPQSpanningTreeExpiryJob<V, L> RAPQSpanningTreeExpiryJob = new RAPQSpanningTreeExpiryJob<V, L>(minTimestamp, productGraph, tree);
            futures.add(completionService.submit(RAPQSpanningTreeExpiryJob));
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
        this.treeSizeHistogram = metricRegistry.histogram("tree-size-histogram");
    }

    private static class RAPQSpanningTreeExpiryJob<V,L> implements Callable<Void> {

        private Long minTimestamp;
        private ProductGraph<V,L> productGraph;

        private AbstractSpanningTree<V> tree;

        public RAPQSpanningTreeExpiryJob(Long minTimestamp, ProductGraph<V,L> productGraph, AbstractSpanningTree<V> tree) {
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
