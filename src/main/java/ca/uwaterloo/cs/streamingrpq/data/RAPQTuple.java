package ca.uwaterloo.cs.streamingrpq.data;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class RAPQTuple implements Tuple {

    ProductNode targetNode;

    int source;
    int sourceState;

    private int hash = 0;

    public RAPQTuple(int source, int sourceState, ProductNode targetNode) {
        this.targetNode = targetNode;
        this.source = source;
        this.sourceState = sourceState;
    }

    public RAPQTuple(int source, ProductNode targetNode) {
        this(source, 0, targetNode);
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

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof RAPQTuple)) {
            return false;
        }

        RAPQTuple tuple = (RAPQTuple) o;

        return tuple.source == source && tuple.targetNode.equals(targetNode);
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int h = hash;
        if(h == 0) {
            h = 17;
            h = 31 * h + source;
            h = 31 * h + targetNode.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("<").append(source).
                append(",").append(targetNode.vertex).append(">  to: ").
                append(targetNode.state).toString();
    }
}
