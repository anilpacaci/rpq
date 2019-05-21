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

    Integer source;
    Integer target;
    Integer sourceState;
    Integer targetState;
    Set<ProductNode> predecessors;

    public Tuple(Integer source, Integer target, Integer sourceState, Integer targetState) {
        this.source = source;
        this.target = target;
        this.sourceState = sourceState;
        this.targetState = targetState;
        predecessors = new HashSet<>();
    }

    public Tuple(Integer source, Integer target, Integer targetState) {
        this(source, target, 0, targetState);
    }

    public Integer getSource() {
        return source;
    }

    public Integer getTarget() {
        return target;
    }

    public Integer getTargetState() {
        return targetState;
    }


    public static final Attribute<Tuple, Integer> TUPLE_SOURCE = new SimpleAttribute<Tuple, Integer>("source") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.source; }
    };

    public static final Attribute<Tuple, Integer> TUPLE_TARGET = new SimpleAttribute<Tuple, Integer>("target") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.target; }
    };

    public static final Attribute<Tuple, Integer> TUPLE_SOURCESTATE = new SimpleAttribute<Tuple, Integer>("sourceState") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.sourceState; }
    };

    public static final Attribute<Tuple, Integer> TUPLE_TARGETSTATE = new SimpleAttribute<Tuple, Integer>("targetState") {
        public Integer getValue(Tuple tuple, QueryOptions queryOptions) { return tuple.targetState; }
    };

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Tuple)) {
            return false;
        }

        Tuple tuple = (Tuple) o;

        return tuple.source.equals(source) && tuple.target.equals(target) && tuple.targetState.equals(targetState);
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + source;
        result = 31 * result + target;
        result = 31 * result + targetState;
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("<").append(source).
                append(",").append(target).append(">  to: ").
                append(targetState).append(" predecessor: ").append(predecessors.size()).toString();
    }

    public void addPredecessor(ProductNode predecessor) {
        this.predecessors.add(predecessor);
    }
}
