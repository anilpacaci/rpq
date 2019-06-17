package ca.uwaterloo.cs.streamingrpq.util.cycle;

public class Edge<N,L> {

    N source, target;
    L label;

    public Edge(N source, N target, L label) {
        this.source = source;
        this.target = target;
        this.label = label;
    }

    public N getSource() {
        return source;
    }

    public void setSource(N source) {
        this.source = source;
    }

    public N getTarget() {
        return target;
    }

    public void setTarget(N target) {
        this.target = target;
    }

    public L getLabel() {
        return label;
    }

    public void setLabel(L label) {
        this.label = label;
    }
}
