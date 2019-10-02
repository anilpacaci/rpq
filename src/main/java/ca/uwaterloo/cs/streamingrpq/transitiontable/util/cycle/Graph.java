package ca.uwaterloo.cs.streamingrpq.transitiontable.util.cycle;

import com.google.common.collect.HashMultimap;

import java.util.Collection;
import java.util.Set;

public class Graph<N, L> {

    private HashMultimap<N, Edge<N,L>> edges;

    public Graph() {
        edges = HashMultimap.create();
    }

    public void addEdge(N source, N target, L label) {
        Edge<N, L> edge = new Edge<N, L>(source, target, label);
        edges.put(source, edge);
    }

    public Collection<N> getVertices() {
        return  edges.keySet();
    }

    public Collection<Edge<N,L>> getChildren(N vertex) {
        return edges.get(vertex);
    }

    public L getLabel(N source, N target) {
        Set<Edge<N, L>> neighbours = edges.get(source);
        return neighbours.stream().filter(e -> e.getTarget().equals(target)).findFirst().get().getLabel();
    }

    public int getVertexCount() {
        return edges.size();
    }
}
