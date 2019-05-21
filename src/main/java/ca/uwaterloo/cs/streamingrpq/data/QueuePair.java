package ca.uwaterloo.cs.streamingrpq.data;

/**
 * Created by anilpacaci on 2019-05-18.
 */
public class QueuePair {

    Tuple tuple;
    ProductNode productNode;

    public QueuePair(Tuple tuple, ProductNode productNode) {
        this.tuple = tuple;
        this.productNode = productNode;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public ProductNode getProductNode() {
        return productNode;
    }

    @Override
    public String toString() {
        return new StringBuilder(this.tuple.toString()).append(" -> pre: ").append(this.productNode.toString()).toString();
    }
}
