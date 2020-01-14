package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractSpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractTreeNode<V, T extends AbstractSpanningTree<V, T, N>, N extends AbstractTreeNode<V, T, N>> {

    protected V vertex;
    protected int state;
    protected long timestamp;
    protected N parent;
    protected Collection<N> children;

    protected AbstractTreeNode(V vertex, int state, N parent, long timestamp) {
        this.vertex = vertex;
        this.state = state;
        this.parent = parent;
        this.children = Collections.newSetFromMap(new ConcurrentHashMap<N, Boolean>());;
        this.timestamp = timestamp;



    }

    public abstract AbstractSpanningTree<V, T, N> getTree();

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
        this.getTree().updateTimestamp(timestamp);
    }

    /**
     * Sets the timestamp of this node to Long.MIN for expiry ro remove after an explicit deletion
     * it does not set the timestamp for the tree
     */
    public void setDeleted() {
        this.timestamp = Long.MIN_VALUE;
    }

    public N getParent() {
        return parent;
    }

    /**
     * Changes the parent of the current node. Removes this node from previous parent's children nodes, and
     * adds it into new parent's children nodes.
     * @param parent new parent. <code>null</code> only if this node is deleted
     */
    public void setParent(N parent) {
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
            this.parent.addChildren((N) this);
        }
    }

    public Collection<N> getChildren() {
        return children;
    }

    public void addChildren(N child) {
        this.children.add(child);
    }
}
