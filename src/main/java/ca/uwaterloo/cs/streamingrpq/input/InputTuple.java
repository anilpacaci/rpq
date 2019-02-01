package ca.uwaterloo.cs.streamingrpq.input;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class InputTuple {

    private int source;
    private int target;
    private char label;

    public InputTuple(int source, int target, char label) {
        this.source = source;
        this.target = target;
        this.label = label;
    }

    public int getSource() {
        return source;
    }

    public int getTarget() {
        return target;
    }

    public char getLabel() {
        return label;
    }
}
