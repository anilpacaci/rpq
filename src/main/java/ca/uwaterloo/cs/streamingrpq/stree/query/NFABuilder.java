package ca.uwaterloo.cs.streamingrpq.stree.query;

public class NFABuilder<T> {

    public NFABuilder() {

    }

    public NFA<T> transition(T label) {
        NFA<T> nfa = new NFA<>();
        nfa.getEntry().addTransition(label, nfa.getExit());
        nfa.getExit().setFinal(true);

        return nfa;
    }

    public NFA<T> epsilonTransition() {
        NFA<T> nfa = new NFA<>();
        nfa.getEntry().addEpsilonTransitions(nfa.getExit());
        nfa.getExit().setFinal(true);

        return nfa;
    }

    public NFA<T> kleeneStar(NFA<T> nfa) {
        nfa.getExit().addEpsilonTransitions(nfa.getEntry());

        return nfa;
    }

    public NFA<T> concenetation(NFA<T> first, NFA<T> second) {
        first.getExit().setFinal(false);
        first.getExit().addEpsilonTransitions(second.getEntry());

        return first;
    }

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
}
