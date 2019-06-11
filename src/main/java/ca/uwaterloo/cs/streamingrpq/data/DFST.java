package ca.uwaterloo.cs.streamingrpq.data;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.Collection;

public interface DFST<T extends Tuple, R> {

    void addTuple(T tuple) throws NoSpaceException;

    Collection<R> retrieveByTarget(int targetVertex, int targetState);

    Collection<R> retrieveByTarget(ProductNode targetNode);

    boolean contains(T tuple);

    boolean contains(ProductNode node);

    int getTupleCount();

    void setMetricRegistry(MetricRegistry registry);
}
