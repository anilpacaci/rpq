package ca.uwaterloo.cs.streamingrpq.stree.query;

public class NFA<T> {

    private State<T> entry;
    private State<T> exit;

    public NFA() {
        entry = new State<>();
        exit = new State<>();
    }


    public State<T> getEntry() {
        return entry;
    }

    public State<T> getExit() {
        return exit;
    }

}
