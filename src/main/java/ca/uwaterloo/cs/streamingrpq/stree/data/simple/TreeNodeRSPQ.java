package ca.uwaterloo.cs.streamingrpq.stree.data.simple;

import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class TreeNodeRSPQ<V> {

    private int hash = 0;

    protected V vertex;
    protected int state;
    protected long timestamp;

    private SpanningTreeRSPQ<V> tree;

    private TreeNodeRSPQ parent;

    private Collection<TreeNodeRSPQ<V>> children;

    private Map<V, Integer> firstMarkings;
    private SetMultimap<V, Integer> currentMarkings;

    protected TreeNodeRSPQ(V vertex, int state, TreeNodeRSPQ parent, SpanningTreeRSPQ t, long timestamp) {
        this.vertex = vertex;
        this.state = state;
        this.parent = parent;
        this.children = new HashSet<>();
        this.tree = t;
        this.timestamp = timestamp;
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

        if(parent != null) {
            this.parent.addChildren(this);
        }
    }

    public SpanningTreeRSPQ<V> getTree() {
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
        this.tree.updateTimestamp(timestamp);
    }

    public TreeNodeRSPQ<V> getParent() {
        return parent;
    }

    /**
     * Changes the parent of the current node. Removes this node from previous parent's children nodes, and
     * adds it into new parent's children nodes.
     * @param parent new parent. <code>null</code> only if this node is deleted
     */
    public void setParent(TreeNodeRSPQ parent) {
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

    public Collection<TreeNodeRSPQ<V>> getChildren() {
        return children;
    }

    private void addChildren(TreeNodeRSPQ child) {
        this.children.add(child);
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
