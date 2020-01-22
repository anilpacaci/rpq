package ca.uwaterloo.cs.streamingrpq.stree.query;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;

/**
 * Created by anilpacaci on 2020-01-21.
 */
public class BricsAutomataBuilder implements AutomataBuilder<Automaton, String> {

    @Override
    public Automaton transition(String label) {
        Automaton resultAutomaton = BasicAutomata.makeChar(label.charAt(0));
        return resultAutomaton;
    }

    @Override
    public Automaton kleeneStar(Automaton nfa) {
        Automaton resultAutomaton = nfa.repeat();
        return resultAutomaton;
    }

    @Override
    public Automaton concenetation(Automaton first, Automaton second) {
        Automaton resultAutomaton = first.concatenate(second);
        return resultAutomaton;
    }

    @Override
    public Automaton alternation(Automaton first, Automaton second) {
        Automaton resultAutomaton = first.union(second);
        return resultAutomaton;
    }

    @Override
    public Automaton inverse(Automaton nfa) {
        return null;
    }
}
