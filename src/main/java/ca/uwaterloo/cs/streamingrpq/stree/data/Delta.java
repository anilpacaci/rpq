package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;

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

    protected void updateTreeNodeIndex(SpanningTree<V> tree, TreeNode<V> treeNode) {
        treeNodeIndex.put(treeNode.hashCode(), tree);
    }
}
