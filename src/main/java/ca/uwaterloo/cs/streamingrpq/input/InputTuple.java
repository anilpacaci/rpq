package ca.uwaterloo.cs.streamingrpq.input;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class InputTuple<S,T,L> {

    private S source;
    private T target;
    private L label;
    private long timestamp;

    private TupleType type;

    /**
     * @{@link Type}, INSERT by default
     * @param source
     * @param target
     * @param label
     */
    public InputTuple(S source, T target, L label) {
        this(source, target, label, 0);
    }

    public InputTuple(S source, T target, L label, long timestamp) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.type = TupleType.INSERT;
        this.timestamp = timestamp;
    }

    /**
     *
     * @param source
     * @param target
     * @param label
     * @param type @{@link TupleType}, INSERT by default
     */
    public InputTuple(S source, T target, L label, long timestamp, TupleType type) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.timestamp = timestamp;
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

    public void setSource(S source) {
        this.source = source;
    }

    public void setTarget(T target) {
        this.target = target;
    }

    public void setLabel(L label) {
        this.label = label;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setType(TupleType type) {
        this.type = type;
    }

    public boolean isDeletion() {
        return this.type == TupleType.DELETE;
    }

    @Override
    public String toString() {
        return new StringBuilder("<").append(this.source).append(",").append(this.target).append(",").append(this.label).append(",").append(this.type).append(">").toString();
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Created by anilpacaci on 2019-02-22.
     */
    public static enum TupleType {
        INSERT,
        DELETE
    }
}
