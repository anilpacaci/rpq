package ca.uwaterloo.cs.streamingrpq.input;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class InputTuple<S,T,L> {

    private S source;
    private T target;
    private L label;

    public InputTuple(S source, T target, L label) {
        this.source = source;
        this.target = target;
        this.label = label;
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
}
