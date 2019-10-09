package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Delta<V> {

    private HashMap<V, SpanningTree> treeIndex;
    private Table<V, Integer, Set<SpanningTree>> nodeToTreeIndex;

    protected Counter treeCounter;
    protected Histogram maintainedTreeHistogram;

    private final Logger LOG = LoggerFactory.getLogger(Delta.class);

    public Delta(int capacity) {
        treeIndex = new HashMap<>(capacity);
        nodeToTreeIndex = HashBasedTable.create(capacity, Constants.EXPECTED_LABELS);
    }

    public SpanningTree getTree(V vertex) {
        SpanningTree tree = treeIndex.get(vertex);
        return tree;
    }

    public Collection<SpanningTree> getTrees() {
        return treeIndex.values();
    }

    public Collection<SpanningTree> getTrees(V vertex, int state) {
        Set<SpanningTree> containingTrees = nodeToTreeIndex.get(vertex, state);
        if(containingTrees == null) {
            containingTrees = new HashSet<>(Constants.EXPECTED_TREES);
            nodeToTreeIndex.put(vertex, state, containingTrees);
        }
        return containingTrees;
    }

    public boolean exists(V vertex) {
        return treeIndex.containsKey(vertex);
    }

    public SpanningTree<V> addTree(V vertex, long timestamp) {
        if(exists(vertex)) {
            // TODO one spanner per root vertex
        }
        SpanningTree<V> tree = new SpanningTree<>(this, vertex, timestamp);
        treeIndex.put(vertex, tree);
        addToTreeNodeIndex(tree, tree.getRootNode());
        treeCounter.inc();
        return tree;
    }

    protected void addToTreeNodeIndex(SpanningTree<V> tree, TreeNode<V> treeNode) {
        Collection<SpanningTree> containingTrees = getTrees(treeNode.getVertex(), treeNode.getState());
        containingTrees.add(tree);
    }

    /**
     * Simply performs a full bFS traversal and re-create all edges
     * @param minTimestamp
     * @param productGraph
     * @param automata
     * @param <L>
     */
    public <L> void batchExpiry(Long minTimestamp, ProductGraph<V,L> productGraph, QueryAutomata<L> automata) {
        // clear both indexes
        nodeToTreeIndex.clear();
        treeIndex.clear();
        LOG.info("Batch expiry at {}, indices are cleard", minTimestamp);

    }

    /**
     * Updates Perform window expiry on each spanning tree
     * @param minTimestamp lower bound on the window interval
     * @param productGraph snapshotGraph
     * @param automata query automata
     * @param <L>
     */
    public <L> void expiry(Long minTimestamp, ProductGraph<V,L> productGraph, QueryAutomata<L> automata) {
        Collection<SpanningTree> trees = treeIndex.values();
        Collection<SpanningTree> treesToBeRemoved = new HashSet<SpanningTree>();
        LOG.info("{} of trees in Delta", trees.size());
        for(SpanningTree<V> tree : trees) {
            if(tree.getMinTimestamp() > minTimestamp) {
                // this tree does not have any node to be deleted, so just skip it
                continue;
            }
            Collection<TreeNode> removedTuples = tree.removeOldEdges(minTimestamp, productGraph, automata);
            // first update treeNode index
            for(TreeNode<V> removedTuple : removedTuples) {
                Collection<SpanningTree> containingTrees = getTrees(removedTuple.getVertex(), removedTuple.getState());
                containingTrees.remove(tree);
                if(containingTrees.isEmpty()) {
                    nodeToTreeIndex.remove(removedTuple.getVertex(), removedTuple.getState());
                }
            }

            // now if the tree has expired simply remove it from tree index
            if(tree.isExpired(minTimestamp)) {
                treesToBeRemoved.add(tree);
            }
        }
        // now update the treeIndex
        for(SpanningTree<V> tree: treesToBeRemoved) {
            TreeNode<V> removedTuple = tree.getRootNode();
            treeIndex.remove(tree.getRootVertex());
            Collection<SpanningTree> containingTrees = getTrees(removedTuple.getVertex(), removedTuple.getState());
            containingTrees.remove(tree);
            if(containingTrees.isEmpty()) {
                nodeToTreeIndex.remove(removedTuple.getVertex(), removedTuple.getState());
            }
            treeCounter.dec();
        }

        LOG.info("Expiry at {}: # of trees {}, # of edges in the productGraph {}", minTimestamp, treeIndex.size(), productGraph.getEdgeCount());
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.treeCounter = metricRegistry.counter("tree-counter");
        this.maintainedTreeHistogram = metricRegistry.histogram("expired-tree-histogram");
    }
}
