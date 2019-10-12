package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.engine.AbstractTreeExpansionJob;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class DeltaRAPQ<V>{

    private Map<V, SpanningTreeRAPQ> treeIndex;
    private Map<Hasher.MapKey<V>, Set<SpanningTreeRAPQ>> nodeToTreeIndex;

    protected Counter treeCounter;
    protected Histogram maintainedTreeHistogram;

    private final Logger LOG = LoggerFactory.getLogger(DeltaRAPQ.class);

    public DeltaRAPQ(int capacity) {
        treeIndex = Maps.newConcurrentMap();
        nodeToTreeIndex = Maps.newConcurrentMap();
    }

    public Collection<SpanningTreeRAPQ> getTrees(V vertex, int state) {
        Set<SpanningTreeRAPQ> containingTrees = nodeToTreeIndex.computeIfAbsent(Hasher.getTreeNodePairKey(vertex, state), key -> Collections.newSetFromMap(new ConcurrentHashMap<SpanningTreeRAPQ, Boolean>()) );
        return containingTrees;
    }

    public boolean exists(V vertex) {
        return treeIndex.containsKey(vertex);
    }



    public SpanningTreeRAPQ<V> addTree(V vertex, long timestamp) {
        if(exists(vertex)) {
            // TODO one spanner per root vertex
        }
        SpanningTreeRAPQ<V> tree = new SpanningTreeRAPQ<>(this, vertex, timestamp);
        treeIndex.put(vertex, tree);
        addToTreeNodeIndex(tree, tree.getRootNode());
        treeCounter.inc();
        return tree;
    }

    public void removeTree(SpanningTreeRAPQ<V> tree) {
        TreeNode<V> rootNode = tree.getRootNode();
        this.treeIndex.remove(rootNode.getVertex());

        Collection<SpanningTreeRAPQ> containingTrees = getTrees(rootNode.getVertex(), rootNode.getState());
        containingTrees.remove(tree);
        if(containingTrees.isEmpty()) {
            nodeToTreeIndex.remove(Hasher.getTreeNodePairKey(rootNode.getVertex(), rootNode.getState()));
        }
        treeCounter.dec();
    }

    protected void addToTreeNodeIndex(SpanningTreeRAPQ<V> tree, TreeNode<V> treeNode) {
        Collection<SpanningTreeRAPQ> containingTrees = getTrees(treeNode.getVertex(), treeNode.getState());
        containingTrees.add(tree);
    }

    protected void removeFromTreeIndex(TreeNode<V> removedNode, SpanningTreeRAPQ<V> tree) {
        Collection<SpanningTreeRAPQ> containingTrees = this.getTrees(removedNode.getVertex(), removedNode.getState());
        containingTrees.remove(tree);
        if(containingTrees.isEmpty()) {
            this.nodeToTreeIndex.remove(Hasher.getTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
        }
    }

    /**
     * Simply performs a full bFS traversal and re-create all edges
     * @param minTimestamp
     * @param productGraph
     * @param <L>
     */
    public <L> void batchExpiry(Long minTimestamp, ProductGraph<V,L> productGraph, CompletionService executorService) {
        // clear both indexes
        nodeToTreeIndex.clear();
        treeIndex.clear();
        LOG.info("Batch expiry at {}, indices are cleard", minTimestamp);

        // retrieve all the nodes with start state 0
        Collection<ProductGraphNode<V>> productGraphNodes = productGraph.getVertices();

        for(ProductGraphNode<V> node : productGraphNodes) {
            if(node.getState() != 0) {
                // create tree only for the nodes with start state 0
                continue;
            }

            Collection<GraphEdge<ProductGraphNode<V>>> edges = productGraph.getForwardEdges(node);
            Optional<Long> maxTimestamp = edges.stream().map(e -> e.getTimestamp()).max(Long::compare);
            if(!maxTimestamp.isPresent() || maxTimestamp.get() <= minTimestamp) {
                // has no valid edge, skip this node
                continue;
            }

            // create a spanning tree
            SpanningTreeRAPQ<V> tree = this.addTree(node.getVertex(), maxTimestamp.get());
            // traverse this tree
            Queue<ProductGraphNode<V>> queue = new LinkedList<>();
            queue.offer(node);

            while(!queue.isEmpty()) {
                ProductGraphNode<V> currentNode = queue.remove();
                TreeNode<V> currentTreeNode = tree.getNode(currentNode.getVertex(), currentNode.getState());

                Collection<GraphEdge<ProductGraphNode<V>>> forwardEdges = productGraph.getForwardEdges(currentNode);
                for(GraphEdge<ProductGraphNode<V>> forwardEdge : forwardEdges) {
                    if(forwardEdge.getTimestamp() <= minTimestamp) {
                        // this edge is not valid, skip it
                        continue;
                    }

                    ProductGraphNode<V> targetNode = forwardEdge.getTarget();
                    if(!tree.exists(targetNode.getVertex(), targetNode.getState())) {
                        tree.addNode(currentTreeNode, targetNode.getVertex(), targetNode.getState(), Long.min(currentTreeNode.getTimestamp(), forwardEdge.getTimestamp()));
                        queue.offer(targetNode);
                    }
                }

            }
            //entire tree is traversed, now add it to the tree index

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
        Collection<SpanningTreeRAPQ> trees = treeIndex.values();
        List<Future<Void>> futures = new ArrayList<>(trees.size());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        LOG.info("{} of trees in DeltaRAPQ", trees.size());
        for(SpanningTreeRAPQ<V> tree : trees) {
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
        this.maintainedTreeHistogram = metricRegistry.histogram("expired-tree-histogram");
    }

    private static class RAPQSpanningTreeExpiryJob<V,L> implements Callable<Void> {

        private Long minTimestamp;
        private ProductGraph<V,L> productGraph;

        private Table<V, Integer, Set<SpanningTreeRAPQ>> nodeToTreeIndex;
        private SpanningTreeRAPQ<V> tree;

        public RAPQSpanningTreeExpiryJob(Long minTimestamp, ProductGraph<V,L> productGraph, SpanningTreeRAPQ<V> tree) {
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
