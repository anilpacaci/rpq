package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import java.util.*;

public class Delta<V> {

    private HashMap<V, SpanningTree> treeIndex;
    private Table<V, Integer, Set<SpanningTree>> treeNodeIndex;


    public Delta(int capacity) {
        treeIndex = new HashMap<>(capacity);
        treeNodeIndex = HashBasedTable.create(capacity, Constants.EXPECTED_TREES);
    }

    public SpanningTree getTree(V vertex) {
        SpanningTree tree = treeIndex.get(vertex);
        return tree;
    }

    public Collection<SpanningTree> getTrees() {
        return treeIndex.values();
    }

    public Collection<SpanningTree> getTrees(V vertex, int state) {
        Set<SpanningTree> containingTrees = treeNodeIndex.get(vertex, state);
        if(containingTrees == null) {
            containingTrees = new HashSet<>();
            treeNodeIndex.put(vertex, state, containingTrees);
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
        for(SpanningTree<V> tree : trees) {
            Iterator<TreeNode> removedTuples = tree.removeOldEdges(minTimestamp, graph, automata).iterator();
            // first update treeNode index
            while(removedTuples.hasNext()) {
                TreeNode<V> removedTuple = removedTuples.next();
                Collection<SpanningTree> containingTrees = getTrees(removedTuple.getVertex(), removedTuple.getState());
                containingTrees.remove(tree);
                if(containingTrees.isEmpty()) {
                    treeNodeIndex.remove(removedTuple.getVertex(), removedTuple.getState());
                }
            }

            // now if the tree has expired simply remove it from tree index
            if(tree.isExpired(minTimestamp)) {
                treesToBeRemoved.add(tree);
            }
        }
        // now update the treeIndex
        for(SpanningTree<V> tree: treesToBeRemoved) {
            treeIndex.remove(tree.getRootVertex());
        }
    }

}
