package ca.uwaterloo.cs.streamingrpq.transitiontable.data;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashMultimap;

import java.util.Collection;


/**
 * Created by anilpacaci on 2019-01-31.
 */
public class ArbitraryDFST implements DFST<RAPQTuple, Integer> {

    private HashMultimap<ProductNode, Integer> targetNodes;
    private Meter tupleCounter;

    public ArbitraryDFST(int capacity, int expectedKeys) {
        targetNodes =  HashMultimap.create(capacity, expectedKeys);
    }

    public void addTuple(RAPQTuple tuple) {

        targetNodes.put(tuple.getTargetNode(), tuple.source);
        tupleCounter.mark();
    }

    public Collection<Integer> retrieveByTarget(int targetVertex, int targetState) {
        ProductNode targetNode = new ProductNode(targetVertex, targetState);
        return targetNodes.get(targetNode);
    }

    public Collection<Integer> retrieveByTarget(ProductNode targetNode) {
        return targetNodes.get(targetNode);
    }

    public boolean contains(RAPQTuple tuple) {
        return targetNodes.containsEntry(tuple.getTargetNode(), tuple.getSource());
    }

    public boolean contains(ProductNode node) {
        return targetNodes.containsKey(node);
    }

    public int getTupleCount() {
        return targetNodes.values().size();
    }

    @Override
    public void setMetricRegistry(MetricRegistry registry) {
        tupleCounter = registry.meter("dfst-counter");
    }

}
