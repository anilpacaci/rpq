package ca.uwaterloo.cs.streamingrpq.data;

/**
 * Created by anilpacaci on 2019-05-18.
 */
public class ProductNode {

    Integer vertex;
    Integer state;

    public ProductNode(Integer vertex, Integer state) {
        this.vertex = vertex;
        this.state = state;
    }

    public Integer getVertex() {
        return vertex;
    }

    public Integer getState() {
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
        int result = 17;
        result = 31 * result + state;
        result = 31 * result + vertex;
        return result;
    }
}
