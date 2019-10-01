package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class SpanningTree<V> {

    private TreeNode<V> rootNode;

    Table<V, Integer, TreeNode> nodeIndex;

    protected SpanningTree(V rootVertex) {
        this.rootNode = new TreeNode<V>(rootVertex, 0, null, this);
        this.nodeIndex = HashBasedTable.create();
        nodeIndex.put(rootVertex, 0, rootNode);
    }


    public void addNode(TreeNode parentNode, V childVertex, int childState, long timestamp) {
        if(parentNode == null) {
            // TODO no object found
        }
        if(parentNode.getTree().equals(this)) {
            // TODO wrong tree
        }

        TreeNode<V> child = new TreeNode<>(childVertex, childState, parentNode, this);
        parentNode.addChildren(child);
        nodeIndex.put(childVertex, childState, child);
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.contains(vertex, state);
    }

    public TreeNode getNode(V vertex, int state) {
        TreeNode node = nodeIndex.get(vertex, state);
        return node;
    }

    public V getRootVertex() {
        return this.rootNode.getVertex();
    }
}
