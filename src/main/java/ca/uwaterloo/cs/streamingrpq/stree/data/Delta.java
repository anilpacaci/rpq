package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Delta<V> {

    private HashMap<V, SpanningTree> treeIndex;
    private Table<V, Integer, Set<SpanningTree>> nodeToTreeIndex;

    private final Logger LOG = LoggerFactory.getLogger(Delta.class);

    public Delta(int capacity) {
        treeIndex = new HashMap<>(capacity);
        nodeToTreeIndex = HashBasedTable.create(capacity, Constants.EXPECTED_TREES);
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
            containingTrees = new HashSet<>();
            nodeToTreeIndex.put(vertex, state, containingTrees);
        }
        return containingTrees;
    }

    public boolean exists(V vertex) {
        return treeIndex.containsKey(vertex);
    }

    public void addTree(V vertex, long timestamp) {
        if(exists(vertex)) {
            // TODO one spanner per root vertex
        }
        SpanningTree<V> tree = new SpanningTree<>(this, vertex, timestamp);
        treeIndex.put(vertex, tree);
        addToTreeNodeIndex(tree, tree.getRootNode());
    }

    protected void addToTreeNodeIndex(SpanningTree<V> tree, TreeNode<V> treeNode) {
        Collection<SpanningTree> containingTrees = getTrees(treeNode.getVertex(), treeNode.getState());
        containingTrees.add(tree);
    }

    /**
     * Updates Perform window expiry on each spanning tree
     * @param minTimestamp lower bound on the window interval
     * @param graph snapshotGraph
     * @param automata query automata
     * @param <L>
     */
    public <L> void expiry(Long minTimestamp, Graph<V,L> graph, QueryAutomata<L> automata) {
        Collection<SpanningTree> trees = treeIndex.values();
        Collection<SpanningTree> treesToBeRemoved = new HashSet<SpanningTree>();
        LOG.info("{} of trees in Delta", trees.size());
        for(SpanningTree<V> tree : trees) {
            if(tree.getMinTimestamp() > minTimestamp) {
                // this tree does not have any node to be deleted, so just skip it
                continue;
            }
            Collection<TreeNode> removedTuples = tree.removeOldEdges(minTimestamp, graph, automata);
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
        }

        LOG.info("Expiry at {}: # of trees {}, # of edges in the graph {}", minTimestamp, treeIndex.size(), graph.getEdgeCount());
    }

}
