package ca.uwaterloo.cs.streamingrpq.transitiontable.data;

/**
 * Created by anilpacaci on 2019-05-18.
 */
public class QueuePair<T extends Tuple, N> {

    T tuple;
    N productNode;

    public QueuePair(T tuple, N productNode) {
        this.tuple = tuple;
        this.productNode = productNode;
    }

    public T getTuple() {
        return tuple;
    }

    public N getProductNode() {
        return productNode;
    }

    @Override
    public String toString() {
        return new StringBuilder(this.tuple.toString()).append(" -> pre: ").append(this.productNode.toString()).toString();
    }
}
