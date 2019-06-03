package ca.uwaterloo.cs.streamingrpq.data;

/**
 * Created by anilpacaci on 2019-05-18.
 */
public class ProductNode {

    int vertex;
    int state;

    private int hash = 0;

    public ProductNode(int vertex, int state) {
        this.vertex = vertex;
        this.state = state;
    }

    public int getVertex() {
        return vertex;
    }

    public int getState() {
        return state;
    }

    @Override

    public boolean equals(Object o) {
        if (o == this) return true;
        if (! (o instanceof ProductNode)) return false;

        ProductNode pair = (ProductNode) o;

        return pair.vertex == this.vertex && pair.state == this.state;
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = 17;
            h = 31 * h + state;
            h = 31 * h + vertex;
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
