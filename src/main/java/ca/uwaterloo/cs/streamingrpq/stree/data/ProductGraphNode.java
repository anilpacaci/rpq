package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.*;

/**
 * Created by anilpacaci on 2019-10-08.
 */
public class ProductGraphNode<V> {

    private V vertex;
    private int state;

    private long minForwardEdgeTimestamp;
    private long minBackwardEdgeTimestamp;

    private Queue<GraphEdge<ProductGraphNode<V>>> forwardEdges;
    private Queue<GraphEdge<ProductGraphNode<V>>> backwardEdges;

    private int hash = 0;

    public ProductGraphNode(V vertex, int state) {
        this.vertex = vertex;
        this.state = state;
        this.minForwardEdgeTimestamp = Long.MAX_VALUE;
        this.minBackwardEdgeTimestamp = Long.MAX_VALUE;

        this.forwardEdges = new ArrayDeque<>(Constants.EXPECTED_NEIGHBOURS);
        this.backwardEdges = new ArrayDeque<>(Constants.EXPECTED_NEIGHBOURS);
    }

    public V getVertex() {
        return vertex;
    }

    public int getState() {
        return state;
    }

    protected void addForwardEdge(GraphEdge<ProductGraphNode<V>> forwardEdge) {
        this.forwardEdges.add(forwardEdge);
        if(forwardEdge.getTimestamp() < minForwardEdgeTimestamp) {
            minForwardEdgeTimestamp = forwardEdge.getTimestamp();
        }
    }

    protected void addBackwardEdge(GraphEdge<ProductGraphNode<V>> backwardEdge) {
        this.backwardEdges.add(backwardEdge);
        if(backwardEdge.getTimestamp() < minBackwardEdgeTimestamp) {
            minBackwardEdgeTimestamp = backwardEdge.getTimestamp();
        }
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getForwardEdges() {
        return forwardEdges;
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getBackwardEdges() {
        return backwardEdges;
    }

    /**
     * removes old edges of the given node and updates the min timestamp
     * @param minTimestamp
     * @return true if this node has no edges remaining and needs to be garbage collected, false otherwise
     */
    public ProductGraphNode<V> removeOldEdges(long minTimestamp) {
        int removedForwardEdges = 0;
        // iterater over forwardEdges
        if (minTimestamp <= this.minForwardEdgeTimestamp) {
            Iterator<GraphEdge<ProductGraphNode<V>>> forwardIterator = forwardEdges.iterator();
            while(forwardIterator.hasNext()) {
                GraphEdge<ProductGraphNode<V>> edge = forwardIterator.next();
                if(edge.getTimestamp() <= minTimestamp) {
                    forwardIterator.remove();
                    removedForwardEdges++;
                } else {
                    this.minForwardEdgeTimestamp = edge.getTimestamp();
                    break;
                }
            }
        }

        // iterate over backward edges
        if (minTimestamp <= this.minBackwardEdgeTimestamp) {
            Iterator<GraphEdge<ProductGraphNode<V>>> backwardIterator = backwardEdges.iterator();
            while(backwardIterator.hasNext()) {
                GraphEdge<ProductGraphNode<V>> edge = backwardIterator.next();
                if(edge.getTimestamp() <= minTimestamp) {
                    backwardIterator.remove();
                } else {
                    this.minBackwardEdgeTimestamp = edge.getTimestamp();
                    break;
                }
            }
        }

        return this;
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
