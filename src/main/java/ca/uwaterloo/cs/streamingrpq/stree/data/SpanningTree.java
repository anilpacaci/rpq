package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class SpanningTree<V> {

    private TreeNode<V> rootNode;

    Table<V, Integer, TreeNode> nodeIndex;

    public SpanningTree(V rootVertex) {
        this.rootNode = new TreeNode<V>(rootVertex, 0, null, this);
        this.nodeIndex = HashBasedTable.create();
    }


    public void addNode(V parentVertex, int parentState, V childVertex, int childState, long timestamp) {
        TreeNode parent = nodeIndex.get(parentVertex, parentState);
        if(parent == null) {
            // TODO no object found
        }

        TreeNode<V> child = new TreeNode<>(childVertex, childState, parent, this);
        nodeIndex.put(childVertex, childState, child);
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.contains(vertex, state);
    }

    public TreeNode getNode(V vertex, int state) {
        TreeNode node = nodeIndex.get(vertex, state);
        return node;
    }
}
