package ca.uwaterloo.cs.streamingrpq.data;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashMultimap;

import java.util.Collection;


/**
 * Created by anilpacaci on 2019-01-31.
 */
public class SimpleDFST implements DFST<RSPQTuple, RSPQTuple> {

    private HashMultimap<ProductNode, RSPQTuple> targetNodes;
    private int capacity;

    private Meter tupleCounter;

    public SimpleDFST(int capacity, int expectedKeys) {
        targetNodes =  HashMultimap.create(capacity, expectedKeys);
        this.capacity = capacity;
    }

    @Override
    public void addTuple(RSPQTuple tuple) throws NoSpaceException {
        if(targetNodes.size() > capacity) {
            throw new NoSpaceException(capacity);
        }
        targetNodes.put(tuple.getTargetNode(), tuple);
        tupleCounter.mark();
    }

    @Override
    public Collection<RSPQTuple> retrieveByTarget(int targetVertex, int targetState) {
        ProductNode targetNode = new ProductNode(targetVertex, targetState);
        return targetNodes.get(targetNode);
    }

    @Override
    public Collection<RSPQTuple> retrieveByTarget(ProductNode targetNode) {
        return targetNodes.get(targetNode);
    }

    @Override
    public boolean contains(RSPQTuple tuple) {
        return targetNodes.containsEntry(tuple.getTargetNode(), tuple);
    }

    @Override
    public boolean contains(ProductNode node) {
        return targetNodes.containsKey(node);
    }

    @Override
    public int getTupleCount() {
        return targetNodes.values().size();
    }

    @Override
    public void setMetricRegistry(MetricRegistry registry) {
        tupleCounter = registry.meter("dfst-counter");
    }

}
