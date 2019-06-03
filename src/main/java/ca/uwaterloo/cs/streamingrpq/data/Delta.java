package ca.uwaterloo.cs.streamingrpq.data;

import com.google.common.collect.HashMultimap;

import java.util.Collection;


/**
 * Created by anilpacaci on 2019-01-31.
 */
public class Delta {

    private HashMultimap<ProductNode, Integer> targetNodes;

    public Delta(int capacity, int expectedKeys) {
        targetNodes =  HashMultimap.create(capacity, expectedKeys);
    }

    public void addTuple(Tuple tuple) {

        targetNodes.put(tuple.getTargetNode(), tuple.source);
    }

    public Collection<Integer> retrieveByTarget(int targetVertex, int targetState) {
        ProductNode targetNode = new ProductNode(targetVertex, targetState);
        return targetNodes.get(targetNode);
    }

    public Collection<Integer> retrieveByTarget(ProductNode targetNode) {
        return targetNodes.get(targetNode);
    }

    public boolean contains(Tuple tuple) {
        return targetNodes.containsEntry(tuple.getTargetNode(), tuple.getSource());
    }

}
