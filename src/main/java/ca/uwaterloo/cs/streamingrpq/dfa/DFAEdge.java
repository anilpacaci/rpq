package ca.uwaterloo.cs.streamingrpq.dfa;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFAEdge {

    private DFANode source, target;
    private Character label;

    public DFAEdge(DFANode source, DFANode target, Character label ){
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

    public Character getLabel() {
        return label;
    }
}
