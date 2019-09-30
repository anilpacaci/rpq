package ca.uwaterloo.cs.streamingrpq.stree.data;

import java.util.Collection;
import java.util.HashSet;

public class TreeNode<V> {

    SpanningTree<V> tree;

    private V vertex;
    private int state;
    private long timestamp;

    private TreeNode parent;

    private Collection<TreeNode> children;

    public TreeNode(V vertex, int state, TreeNode parent, SpanningTree t) {
        this.vertex = vertex;
        this.state = state;
        this.parent = parent;
        this.children = new HashSet<>();
        this.tree = t;
    }

    public SpanningTree<V> getTree() {
        return tree;
    }

    public V getVertex() {
        return vertex;
    }


    public int getState() {
        return state;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public TreeNode getParent() {
        return parent;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public Collection<TreeNode> getChildren() {
        return children;
    }

    protected void addChildren(TreeNode child) {
        this.children.add(child);
        child.setParent(this);
    }

}
