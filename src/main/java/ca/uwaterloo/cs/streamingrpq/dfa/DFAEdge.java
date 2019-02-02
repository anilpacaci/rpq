package ca.uwaterloo.cs.streamingrpq.dfa;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFAEdge<L> {

    private DFANode source, target;
    private L label;

    public DFAEdge(DFANode source, DFANode target, L label ){
        this.source = source;
        this.target = target;
        this.label = label;
    }

    public DFANode getSource() {
        return source;
    }

    public DFANode getTarget() {
        return target;
    }

    public L getLabel() {
        return label;
    }
}
