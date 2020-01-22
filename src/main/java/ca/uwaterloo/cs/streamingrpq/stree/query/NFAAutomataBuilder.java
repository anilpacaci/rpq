package ca.uwaterloo.cs.streamingrpq.stree.query;

import com.google.common.collect.Multimap;
import dk.brics.automaton.BasicAutomata;

public class NFAAutomataBuilder<T> implements AutomataBuilder<NFA<T>, T> {

    public NFAAutomataBuilder() {

    }

    /**
     * Create an NFA with a single transition with given label
     * @param label
     * @return
     */
    @Override
    public NFA<T> transition(T label) {
        NFA<T> nfa = new NFA<>();
        nfa.getEntry().addTransition(label, nfa.getExit());
        nfa.getExit().setFinal(true);

        return nfa;
    }

    /**
     * Create an NFA with a Kleene star over a given NFA
     * @param nfa
     * @return
     */
    @Override
    public NFA<T> kleeneStar(NFA<T> nfa) {
        nfa.getEntry().addEpsilonTransitions(nfa.getExit());
        nfa.getExit().addEpsilonTransitions(nfa.getEntry());

        return nfa;
    }

    /**
     * Creates a concenatation NFA from given two NFAs
     * @param first
     * @param second
     * @return
     */
    @Override
    public NFA<T> concenetation(NFA<T> first, NFA<T> second) {
        first.getExit().setFinal(false);
        first.getExit().addEpsilonTransitions(second.getEntry());

        return new NFA<>(first.getEntry(), second.getExit());
    }

    /**
     * Creates a concenatation NFA from given two NFAs
     * @param first
     * @param second
     * @return
     */
    @Override
    public NFA<T> alternation(NFA<T> first, NFA<T> second) {
        first.getExit().setFinal(false);
        second.getExit().setFinal(false);

        NFA<T> newNFA = new NFA<>();

        newNFA.getEntry().addEpsilonTransitions(first.getEntry());
        newNFA.getEntry().addEpsilonTransitions(second.getEntry());

        first.getExit().addEpsilonTransitions(newNFA.getExit());
        second.getExit().addEpsilonTransitions(newNFA.getExit());

        newNFA.getExit().setFinal(true);

        return newNFA;
    }

    @Override
    public NFA<T> inverse(NFA<T> nfa) {
        //TODO implement a reverse logic
        nfa.getExit().setFinal(false);
        nfa.getEntry().setFinal(true);

        Multimap<T, State> transitions = nfa.getEntry().getTransitions();

        NFA<T> inverseNFA = new NFA<>();

        return inverseNFA;
    }
}
