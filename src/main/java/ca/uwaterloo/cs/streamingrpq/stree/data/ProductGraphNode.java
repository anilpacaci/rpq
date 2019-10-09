package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by anilpacaci on 2019-10-08.
 */
public class ProductGraphNode<V> {

    private V vertex;
    private int state;

    private Set<GraphEdge<ProductGraphNode<V>>> forwardEdges;
    private Set<GraphEdge<ProductGraphNode<V>>> backwardEdges;

    private int hash = 0;

    public ProductGraphNode(V vertex, int state) {
        this.vertex = vertex;
        this.state = state;

        this.forwardEdges = new HashSet<>(Constants.EXPECTED_NEIGHBOURS);
        this.backwardEdges = new HashSet<>(Constants.EXPECTED_NEIGHBOURS);
    }

    public V getVertex() {
        return vertex;
    }

    public int getState() {
        return state;
    }

    protected void addForwardEdge(GraphEdge<ProductGraphNode<V>> forwardEdge) {
        this.forwardEdges.add(forwardEdge);
    }

    protected void addBackwardEdge(GraphEdge<ProductGraphNode<V>> backwardEdge) {
        this.backwardEdges.add(backwardEdge);
    }

    protected void removeForwardEdge(GraphEdge<ProductGraphNode<V>> forwardEdge) {
        this.forwardEdges.remove(forwardEdge);
    }

    protected void removeBackwardEdge(GraphEdge<ProductGraphNode<V>> backwardEdge) {
        this.backwardEdges.remove(backwardEdge);
    }

    public Set<GraphEdge<ProductGraphNode<V>>> getForwardEdges() {
        return forwardEdges;
    }

    public Set<GraphEdge<ProductGraphNode<V>>> getBackwardEdges() {
        return backwardEdges;
    }

    @Override

    public boolean equals(Object o) {
        if (o == this) return true;
        if (! (o instanceof ProductGraphNode)) return false;

        ProductGraphNode pair = (ProductGraphNode) o;

        return pair.vertex == this.vertex && pair.state == this.state;
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = 17;
            h = 31 * h + state;
            h = 31 * h + vertex.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return new StringBuilder("<").append(this.vertex)
                .append(",").append(this.state).append(">").toString();
    }
}
