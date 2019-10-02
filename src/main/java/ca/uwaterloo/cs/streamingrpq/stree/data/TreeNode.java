package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;

import java.util.Collection;
import java.util.HashSet;

public class TreeNode<V> {

    private int hash = 0;

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

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TreeNode)) {
            return false;
        }

        TreeNode tuple = (TreeNode) o;

        return tuple.vertex == vertex && tuple.state == state;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if(h == 0) {
            h = Hasher.TreeNodeHasher(vertex.hashCode(), state);
            hash = h;
        }
        return h;
    }
}
