package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractSpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractTreeNode<V> {

    protected V vertex;
    protected int state;
    protected long timestamp;
    protected AbstractTreeNode<V> parent;
    protected Collection<AbstractTreeNode<V>> children;

    protected AbstractTreeNode(V vertex, int state, AbstractTreeNode<V> parent, long timestamp) {
        this.vertex = vertex;
        this.state = state;
        this.parent = parent;
        this.children = Collections.newSetFromMap(new ConcurrentHashMap<AbstractTreeNode<V>, Boolean>());;
        this.timestamp = timestamp;


        if(parent != null) {
            this.parent.addChildren(this);
        }
    }

    public abstract AbstractSpanningTree<V> getTree();

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

    public AbstractTreeNode<V> getParent() {
        return parent;
    }

    /**
     * Changes the parent of the current node. Removes this node from previous parent's children nodes, and
     * adds it into new parent's children nodes.
     * @param parent new parent. <code>null</code> only if this node is deleted
     */
    public void setParent(AbstractTreeNode<V> parent) {
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

    public Collection<? extends AbstractTreeNode> getChildren() {
        return children;
    }

    public void addChildren(AbstractTreeNode<V> child) {
        this.children.add(child);
    }
}
