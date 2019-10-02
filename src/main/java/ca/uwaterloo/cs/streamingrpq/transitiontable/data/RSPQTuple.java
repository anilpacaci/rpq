package ca.uwaterloo.cs.streamingrpq.transitiontable.data;

import com.google.common.collect.HashMultimap;

import java.util.HashMap;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class RSPQTuple implements Tuple {

    ProductNode targetNode;

    RSPQTuple parentNode;

    HashMap<Integer, Integer> firstMarkings;
    HashMultimap<Integer, Integer> currentMarkings;

    int source;
    int sourceState;

    private int hash = 0;

    private RSPQTuple(int source, int sourceState, ProductNode targetNode, RSPQTuple parent) {
        this.parentNode = parent;
        this.targetNode = targetNode;
        this.source = source;
        this.sourceState = sourceState;

        if(parent == null) {
            firstMarkings = new HashMap<>();
            currentMarkings = HashMultimap.create();
            currentMarkings.put(source, sourceState);
            firstMarkings.putIfAbsent(source, sourceState);
        } else {
            firstMarkings = new HashMap<>(parent.firstMarkings);
            currentMarkings = HashMultimap.create(parent.currentMarkings);
        }

        currentMarkings.put(targetNode.getVertex(), targetNode.getState());
        firstMarkings.putIfAbsent(targetNode.getVertex(), targetNode.getState());
    }

    public RSPQTuple(int source, ProductNode targetNode) {
        this(source, 0, targetNode, null);
    }

    @Override
    public int getSource() {
        return source;
    }

    @Override
    public int getTarget() {
        return targetNode.vertex;
    }

    @Override
    public int getTargetState() {
        return targetNode.state;
    }

    @Override
    public ProductNode getTargetNode() {
        return targetNode;
    }

    public RSPQTuple getParentNode() {
        return parentNode;
    }

    public boolean containsCM(int target, int targetState) {
        return currentMarkings.containsEntry(target, targetState);
    }

    public Integer getFirstCM(int vertex) {
        return firstMarkings.get(vertex);
    }

    public RSPQTuple extend(ProductNode node) {
        RSPQTuple extension = new RSPQTuple(this.source, 0, node, this);
        return extension;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof RSPQTuple)) {
            return false;
        }

        RSPQTuple tuple = (RSPQTuple) o;

        return tuple.source == source &&
                tuple.targetNode.equals(targetNode) &&
                tuple.firstMarkings.equals(firstMarkings) &&
                tuple.currentMarkings.equals(currentMarkings);
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int h = hash;
        if(h == 0) {
            h = 17;
            h = 31 * h + source;
            h = 31 * h + targetNode.hashCode();
            h = 31 * h + firstMarkings.hashCode();
            h = 31 * h + currentMarkings.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("").append(source).
                append(" -> ").append(targetNode.vertex).append(",").
                append(targetNode.state).toString();
    }
}
