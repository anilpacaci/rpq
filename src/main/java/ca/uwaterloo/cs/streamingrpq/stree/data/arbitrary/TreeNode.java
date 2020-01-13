package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractSpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractTreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class TreeNode<V> extends AbstractTreeNode<V> {

    private int hash = 0;

    private SpanningTreeRAPQ tree;


    protected TreeNode(V vertex, int state, TreeNode parent, SpanningTreeRAPQ<V> t, long timestamp) {
        super(vertex, state, parent, timestamp);

        // set the containing spanning tree
        this.tree = t;
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
