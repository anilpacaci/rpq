package ca.uwaterloo.cs.streamingrpq.stree.query;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

public class State<T> {

    private boolean isFinal;

    private Multimap<T, State> transitions;
    private Set<State> emptyTransitions;

    public State() {
        this(false);
    }

    public State(boolean isFinal) {
        this.isFinal = isFinal;
        transitions = HashMultimap.create();
        emptyTransitions = Sets.newHashSet();
    }

    public void addTransition(T label, State next) {
        transitions.put(label, next);
    }

    public void addEpsilonTransitions(State next) {
        emptyTransitions.add(next);
    }

    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }

    public Multimap<T, State> getTransitions() {
        return transitions;
    }

    public Collection<State> getTransitions(T label) {
        return transitions.get(label);
    }

    public Set<State> getEmptyTransitions() {
        return emptyTransitions;
    }
}
