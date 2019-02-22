package ca.uwaterloo.cs.streamingrpq.input;

import ca.uwaterloo.cs.streamingrpq.core.TupleType;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class InputTuple<S,T,L> {

    private S source;
    private T target;
    private L label;

    private TupleType type;

    /**
     * @{@link Type}, INSERT by default
     * @param source
     * @param target
     * @param label
     */
    public InputTuple(S source, T target, L label) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.type = TupleType.INSERT;
    }

    /**
     *
     * @param source
     * @param target
     * @param label
     * @param type @{@link TupleType}, INSERT by default
     */
    public InputTuple(S source, T target, L label, TupleType type) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.type = type;
    }

    public S getSource() {
        return source;
    }

    public T getTarget() {
        return target;
    }

    public L getLabel() {
        return label;
    }

    public boolean isDeletion() {
        return this.type == TupleType.DELETE;
    }

}
