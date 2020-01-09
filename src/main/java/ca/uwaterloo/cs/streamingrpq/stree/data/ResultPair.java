package ca.uwaterloo.cs.streamingrpq.stree.data;

public class ResultPair<V> {

    private final V source;
    private final V target;

    private boolean isDeletion;

    public ResultPair(final V source, final V target) {
        this.source = source;
        this.target = target;
        this.isDeletion = false;
    }

    public ResultPair(final V source, final V target, boolean isDeletion) {
        this.source = source;
        this.target = target;
        this.isDeletion = isDeletion;
    }

    public V getSource() {
        return source;
    }

    public V getTarget() {
        return target;
    }

    public boolean isDeletion() {
        return isDeletion;
    }
}
