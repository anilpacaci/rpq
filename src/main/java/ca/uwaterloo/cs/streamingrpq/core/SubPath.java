package ca.uwaterloo.cs.streamingrpq.core;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class SubPath {

    Integer source;
    Integer target;
    Integer sourceState;
    int counter;

    public SubPath(Integer source, Integer target, Integer sourceState) {
        this.source = source;
        this.target = target;
        this.sourceState = sourceState;
        this.counter = 1;
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

    public int getCounter() {
        return counter;
    }

    public void increment() {
        counter++;
    }

    public void decrement() {
        counter--;
    }

    public static final Attribute<SubPath, Integer> TUPLE_SOURCE = new SimpleAttribute<SubPath, Integer>("source") {
        public Integer getValue(SubPath subPath, QueryOptions queryOptions) { return subPath.source; }
    };

    public static final Attribute<SubPath, Integer> TUPLE_TARGET = new SimpleAttribute<SubPath, Integer>("target") {
        public Integer getValue(SubPath subPath, QueryOptions queryOptions) { return subPath.target; }
    };

    public static final Attribute<SubPath, Integer> TUPLE_SOURCESTATE = new SimpleAttribute<SubPath, Integer>("sourceState") {
        public Integer getValue(SubPath subPath, QueryOptions queryOptions) { return subPath.sourceState; }
    };

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof SubPath)) {
            return false;
        }

        SubPath subPath = (SubPath) o;

        return subPath.source.equals(source) && subPath.target.equals(target) && subPath.sourceState.equals(sourceState);
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + source;
        result = 31 * result + target;
        result = 31 * result + sourceState;
        return result;
    }
}
