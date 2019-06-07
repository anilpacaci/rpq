package ca.uwaterloo.cs.streamingrpq.data;

import com.google.common.collect.HashMultimap;

import java.util.Collection;


/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFST {

    private HashMultimap<ProductNode, RSPQTuple> targetNodes;
    private int capacity;

    public DFST(int capacity, int expectedKeys) {
        targetNodes =  HashMultimap.create(capacity, expectedKeys);
        this.capacity = capacity;
    }

    public void addTuple(RSPQTuple tuple) throws NoSpaceException {
        if(targetNodes.size() > capacity) {
            throw new NoSpaceException(capacity);
        }
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

    public boolean contains(ProductNode node) {
        return targetNodes.containsKey(node);
    }

    public int getTupleCount() {
        return targetNodes.values().size();
    }

}
