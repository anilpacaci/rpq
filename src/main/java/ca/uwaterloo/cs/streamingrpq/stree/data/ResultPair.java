package ca.uwaterloo.cs.streamingrpq.stree.data;

public class ResultPair<V> {

    private final V source;
    private final V target;

    public ResultPair(final V source, final V target) {
        this.source = source;
        this.target = target;
    }

    public V getSource() {
        return source;
    }

    public V getTarget() {
        return target;
    }
}
