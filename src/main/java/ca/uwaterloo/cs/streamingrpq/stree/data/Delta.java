package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class Delta<V> {

    private HashMap<V, SpanningTree> treeIndex;
    private Multimap<Integer, SpanningTree> treeNodeIndex;


    public Delta(int capacity) {
        treeIndex = new HashMap<>(capacity);
        treeNodeIndex = HashMultimap.create(capacity, Constants.EXPECTED_TREES);
    }

    public SpanningTree getTree(V vertex) {
        SpanningTree tree = treeIndex.get(vertex);
        return tree;
    }

    public Collection<SpanningTree> getTrees() {
        return treeIndex.values();
    }

    public Collection<SpanningTree> getTrees(V vertex, int state) {
        return  treeNodeIndex.get(Hasher.TreeNodeHasher(vertex.hashCode(), state));
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
    }

    protected void addToTreeNodeIndex(SpanningTree<V> tree, TreeNode<V> treeNode) {
        treeNodeIndex.put(treeNode.hashCode(), tree);
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
                treeNodeIndex.remove(removedTuple.hashCode(), tree);
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
