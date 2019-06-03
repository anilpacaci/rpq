package ca.uwaterloo.cs.streamingrpq.data;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class Tuple {

    ProductNode targetNode;

    int source;
    int sourceState;

    private int hash = 0;

    public Tuple(int source, int sourceState, ProductNode targetNode) {
        this.targetNode = targetNode;
        this.source = source;
        this.sourceState = sourceState;
    }

    public Tuple(int source, int target, int sourceState, int targetState) {
        this(source, sourceState, new ProductNode(target, targetState));
    }

    public Tuple(int source, int target, int targetState) {
        this(source, target, 0, targetState);
    }

    public Tuple(int source, ProductNode targetNode) {
        this(source, 0, targetNode);
    }

    public int getSource() {
        return source;
    }

    public int getTarget() {
        return targetNode.vertex;
    }

    public int getTargetState() {
        return targetNode.state;
    }

    public ProductNode getTargetNode() {
        return targetNode;
    }

    public static final Attribute<Tuple, Integer> TUPLE_SOURCE = new SimpleAttribute<Tuple, Integer>("source") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.source; }
    };

    public static final Attribute<Tuple, Integer> TUPLE_TARGET = new SimpleAttribute<Tuple, Integer>("target") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.targetNode.state; }
    };

    public static final Attribute<Tuple, Integer> TUPLE_SOURCESTATE = new SimpleAttribute<Tuple, Integer>("sourceState") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.sourceState; }
    };

    public static final Attribute<Tuple, Integer> TUPLE_TARGETSTATE = new SimpleAttribute<Tuple, Integer>("targetState") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.targetNode.vertex; }
    };

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Tuple)) {
            return false;
        }

        Tuple tuple = (Tuple) o;

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
