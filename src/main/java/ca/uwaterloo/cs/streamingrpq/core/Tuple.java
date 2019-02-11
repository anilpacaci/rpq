package ca.uwaterloo.cs.streamingrpq.core;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class Tuple {

    Integer source;
    Integer target;
    Integer sourceState;

    public Tuple(Integer source, Integer target, Integer sourceState) {
        this.source = source;
        this.target = target;
        this.sourceState = sourceState;
    }

    public Integer getSource() {
        return source;
    }

    public Integer getTarget() {
        return target;
    }

    public Integer getSourceState() {
        return sourceState;
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

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Tuple)) {
            return false;
        }

        Tuple tuple = (Tuple) o;

        return tuple.source.equals(source) && tuple.target.equals(target) && tuple.sourceState.equals(sourceState);
    }

    //Idea from effective Java : Item 9
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + source;
        result = 31 * result + target;
        result = 31 * result + sourceState;
        return result;
    }
}
