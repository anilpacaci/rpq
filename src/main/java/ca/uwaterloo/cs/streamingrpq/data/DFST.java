package ca.uwaterloo.cs.streamingrpq.data;

import com.google.common.collect.HashMultimap;

import java.util.Collection;


/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFST {

    private HashMultimap<ProductNode, RSPQTuple> targetNodes;

    public DFST(int capacity, int expectedKeys) {
        targetNodes =  HashMultimap.create(capacity, expectedKeys);
    }

    public void addTuple(RSPQTuple tuple) {

        targetNodes.put(tuple.getTargetNode(), tuple);
    }

    public Collection<RSPQTuple> retrieveByTarget(int targetVertex, int targetState) {
        ProductNode targetNode = new ProductNode(targetVertex, targetState);
        return targetNodes.get(targetNode);
    }

    public Collection<RSPQTuple> retrieveByTarget(ProductNode targetNode) {
        return targetNodes.get(targetNode);
    }

    public boolean contains(RSPQTuple tuple) {
        return targetNodes.containsEntry(tuple.getTargetNode(), tuple);
    }

    public int getTupleCount() {
        return targetNodes.values().size();
    }

}
