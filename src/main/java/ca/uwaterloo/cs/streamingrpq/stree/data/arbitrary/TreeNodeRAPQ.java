package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractTreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;

public class TreeNodeRAPQ<V> extends AbstractTreeNode<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> {

    private int hash = 0;

    private SpanningTreeRAPQ tree;


    protected TreeNodeRAPQ(V vertex, int state, TreeNodeRAPQ parent, SpanningTreeRAPQ<V> t, long timestamp) {
        super(vertex, state, parent, timestamp);

        // set the containing spanning tree
        this.tree = t;
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
        if (!(o instanceof TreeNodeRAPQ)) {
            return false;
        }

        TreeNodeRAPQ tuple = (TreeNodeRAPQ) o;

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
