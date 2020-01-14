package ca.uwaterloo.cs.streamingrpq.stree.data.simple;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractTreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TreeNodeRSPQ<V> extends AbstractTreeNode<V, SpanningTreeRSPQ<V>, TreeNodeRSPQ<V>> {

    private int hash = 0;

    SpanningTreeRSPQ<V> tree;

    private Map<V, Integer> firstMarkings;
    private SetMultimap<V, Integer> currentMarkings;

    protected TreeNodeRSPQ(V vertex, int state, TreeNodeRSPQ parent, SpanningTreeRSPQ<V> t, long timestamp) {
        super(vertex, state, parent, timestamp);

        // set the containing spanning tree
        this.tree = t;

        // set this as a child of the parent if it is not null
        if(parent != null) {
            this.firstMarkings = Maps.newHashMap(parent.firstMarkings);
            this.currentMarkings = HashMultimap.create(parent.currentMarkings);
        } else {
            this.firstMarkings = Maps.newHashMap();
            this.currentMarkings = HashMultimap.create();
        }

        // populate with root node
        this.firstMarkings.putIfAbsent(vertex, state);
        this.currentMarkings.put(vertex, state);
    }

    @Override
    public SpanningTreeRSPQ<V> getTree() {
        return tree;
    }


    /**
     * Check whether current markings contain this pair of vertex-state
     * @param vertex
     * @param state
     * @return true if this vertex has already been visited on this state on the path to this node from the root
     */
    public boolean containsCM(V vertex, int state) {
        return currentMarkings.containsEntry(vertex, state);
    }

    /**
     * The first state this vertex is visited on the path from root to this node
     * @param vertex
     * @return null if no such vertex exists
     */
    public Integer getFirstCM(V vertex) {
        return firstMarkings.get(vertex);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TreeNodeRSPQ)) {
            return false;
        }

        TreeNodeRSPQ tuple = (TreeNodeRSPQ) o;

        return tuple.vertex.equals(vertex) &&
                tuple.state == state &&
                tuple.firstMarkings.equals(firstMarkings) &&
                tuple.currentMarkings.equals(currentMarkings);
    }

    @Override
    public int hashCode() {
        int h = hash;
        if(h == 0) {
            h = 17;
            h = 31 * h + vertex.hashCode();
            h = 31 * h + state;
            h = 31 * h + firstMarkings.hashCode();
            h = 31 * h + currentMarkings.hashCode();
            hash = h;
        }
        return h;
    }

}
