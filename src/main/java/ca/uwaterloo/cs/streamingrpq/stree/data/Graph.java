package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.LinkedList;

public class Graph<V,L> {
    private Multimap<V, GraphEdge<V,L>> forwardAdjacency;
    private Multimap<V, GraphEdge<V,L>> backwardAdjacency;


    private LinkedList<GraphEdge<V,L>> timeOrderedEdges;

    public Graph(int capacity) {
        forwardAdjacency = HashMultimap.create(capacity, Constants.EXPECTED_NEIGHBOURS);
        backwardAdjacency = HashMultimap.create(capacity, Constants.EXPECTED_NEIGHBOURS);
        timeOrderedEdges = new LinkedList<GraphEdge<V,L>>();
    }

    public void addEdge(V source, V target, L label, long timestamp) {
        GraphEdge<V, L> forwardEdge = new GraphEdge<>(source, target, label, timestamp);
        forwardAdjacency.put(source, forwardEdge);
        backwardAdjacency.put(target, new GraphEdge<>(source, target, label, timestamp));

        timeOrderedEdges.addLast(forwardEdge);
    }

    private void removeEdgeFromHashIndexes(V source, V target, L label) {
        GraphEdge<V, L> forwardEdge = new GraphEdge<>(source, target, label, 0);
        forwardAdjacency.remove(source, forwardEdge);
        backwardAdjacency.remove(target, new GraphEdge<>(source, target, label, 0));
    }

    public Collection<GraphEdge<V, L>> getForwardEdges(V source) {
        return forwardAdjacency.get(source);
    }

    public Collection<GraphEdge<V, L>> getBackwardEdges(V source) {
        return backwardAdjacency.get(source);
    }

    /**
     * removes old edges from the graph, used during window management
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     */
    public void removeOldEdges(long minTimestamp) {
        // it suffices to linearly scan from the oldest edge as we assume ordered arrival
        while(timeOrderedEdges.peekFirst() != null) {
            GraphEdge<V, L> oldestEdge = timeOrderedEdges.getFirst();
            if(oldestEdge.getTimestamp() <= minTimestamp) {
                timeOrderedEdges.removeFirst();
                removeEdgeFromHashIndexes(oldestEdge.getSource(), oldestEdge.getTarget(), oldestEdge.getLabel());
            } else {
                // as we assume ordered arrival, we can stop the search
                break;
            }
        }
    }
}
