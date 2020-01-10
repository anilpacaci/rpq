package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractTreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class TreeNode<V> extends AbstractTreeNode<V> {

    private int hash = 0;


    protected TreeNode() {

    }

    protected TreeNode(V vertex, int state, TreeNode parent, SpanningTreeRAPQ t, long timestamp) {
        this.vertex = vertex;
        this.state = state;
        this.parent = parent;
        this.children = Collections.newSetFromMap(new ConcurrentHashMap<AbstractTreeNode<V>, Boolean>());
        this.tree = t;
        this.timestamp = timestamp;
        // set this as a child of the parent if it is not null
        if(parent != null) {
            this.parent.addChildren(this);
        }
    }

    @Override
    public SpanningTreeRAPQ<V> getTree() {
        return tree;
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
