package ca.uwaterloo.cs.streamingrpq.stree.data;

/**
 * Created by anilpacaci on 2019-10-08.
 */
public class ProductGraphNode<V> {

    private V vertex;
    private int state;

    private int hash = 0;

    public ProductGraphNode(V vertex, int state) {
        this.vertex = vertex;
        this.state = state;
    }

    public V getVertex() {
        return vertex;
    }

    public int getState() {
        return state;
    }

    @Override

    public boolean equals(Object o) {
        if (o == this) return true;
        if (! (o instanceof ProductGraphNode)) return false;

        ProductGraphNode pair = (ProductGraphNode) o;

        return pair.vertex == this.vertex && pair.state == this.state;
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = 17;
            h = 31 * h + state;
            h = 31 * h + vertex.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return new StringBuilder("<").append(this.vertex)
                .append(",").append(this.state).append(">").toString();
    }
}
