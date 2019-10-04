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

    protected TreeNode(V vertex, int state, TreeNode parent, SpanningTree t, long timestamp) {
        this.vertex = vertex;
        this.state = state;
        this.parent = parent;
        this.children = new HashSet<>();
        this.tree = t;
        this.timestamp = timestamp;
        // set this as a child of the parent if it is not null
        if(parent != null) {
            this.parent.addChildren(this);
        }
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

    /**
     * Changes the parent of the current node. Removes this node from previous parent's children nodes, and
     * adds it into new parent's children nodes.
     * @param parent new parent. <code>null</code> only if this node is deleted
     */
    public void setParent(TreeNode parent) {
        // remove this node from previous parent
        if(this.parent != null) {
            this.parent.children.remove(this);
        }
        // set a new parent
        this.parent = parent;
        // if it is set null, then it is time to remove this node
        if(this.parent != null) {
            // add this as a child to new parent
            if (parent != null) ;
            this.parent.addChildren(this);
        }
    }

    public Collection<TreeNode> getChildren() {
        return children;
    }

    private void addChildren(TreeNode child) {
        this.children.add(child);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TreeNode)) {
            return false;
        }

        TreeNode tuple = (TreeNode) o;

        return tuple.vertex.equals(vertex) && tuple.state == state;
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

    @Override
    public String toString() {
        return new StringBuilder().append("Node:").append(getVertex()).append(",").append(getState()).append("-TS:").append(getTimestamp()).toString();
    }
}
