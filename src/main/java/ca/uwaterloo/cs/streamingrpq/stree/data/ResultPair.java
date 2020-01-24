package ca.uwaterloo.cs.streamingrpq.stree.data;

public class ResultPair<V> {

    private int hash = 0;

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

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = 17;
            h = 31 * h + source.hashCode();
            h = 31 * h + target.hashCode();
            hash = h;
        }
        return h;
    }
}
