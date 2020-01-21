package ca.uwaterloo.cs.streamingrpq.stree.query;

public class NFA<T> {

    private State<T> entry;
    private State<T> exit;

    public NFA() {
        entry = new State<>();
        exit = new State<>();
    }

    public NFA(State<T> entry, State<T>  exit) {
        this.entry = entry;
        this.exit = exit;
    }


    public State<T> getEntry() {
        return entry;
    }

    public State<T> getExit() {
        return exit;
    }

}
