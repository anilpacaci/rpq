package ca.uwaterloo.cs.streamingrpq.stree.query;

import com.google.common.collect.Maps;
import dk.brics.automaton.Automaton;

import java.util.Map;
import java.util.Set;

public class BricsAutomata extends Automata<String> {

    private Map<Character, String> reverseLabelMappings;
    private Map<String, Character> labelMappings;

    private Automaton automaton;

    public BricsAutomata(Automaton automaton, Map<String, Character> labelMappings) {
        super();
        this.labelMappings = labelMappings;
        reverseLabelMappings = Maps.newHashMap();
        this.automaton = automaton;
    }

    public void finalize() {
        // determinize and minimizes the given automaton
        automaton.minimize();


    }

    @Override
    public boolean isFinalState(int state) {
        return false;
    }

    @Override
    public Map<Integer, Integer> getTransition(String label) {
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
    public Set<String> getAlphabet() {
        return null;
    }
}
