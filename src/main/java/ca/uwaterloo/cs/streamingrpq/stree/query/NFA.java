package ca.uwaterloo.cs.streamingrpq.stree.query;

import java.util.Map;
import java.util.Set;

public class NFA<T> extends Automata<T> {

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

    @Override
    public boolean isFinalState(int state) {
        return false;
    }

    @Override
    public Map<Integer, Integer> getTransition(T label) {
        return null;
    }

    @Override
    public int getNumOfStates() {
        return 0;
    }

    @Override
    public Set<Integer> getFinalStates() {
        return null;
    }

    @Override
    public Set<T> getAlphabet() {
        return null;
    }
}
