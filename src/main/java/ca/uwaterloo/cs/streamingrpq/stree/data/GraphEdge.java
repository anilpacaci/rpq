package ca.uwaterloo.cs.streamingrpq.stree.data;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public class GraphEdge<V,L> {

    private int h = 0;

    private V source;
    private V target;
    private L label;
    private long timestamp;

    public GraphEdge(V source, V target, L label, long timestamp) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.timestamp = timestamp;
    }

    public V getSource() {
        return source;
    }

    public V getTarget() {
        return target;
    }

    public L getLabel() {
        return label;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraphEdge<?, ?> graphEdge = (GraphEdge<?, ?>) o;

        if (!source.equals(graphEdge.source)) return false;
        if (!target.equals(graphEdge.target)) return false;
        return label.equals(graphEdge.label);
    }

    @Override
    public int hashCode() {
        if(h == 0) {
            int result = source.hashCode();
            result = 31 * result + target.hashCode();
            result = 31 * result + label.hashCode();
            h = result;
        }
        return h;
    }
}
