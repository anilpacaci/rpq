package ca.uwaterloo.cs.streamingrpq.stree.query;

import com.google.common.collect.Maps;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;

import java.nio.file.Path;
import java.util.Map;

/**
 * Created by anilpacaci on 2020-01-21.
 *
 * It constructs a Brics Automaton using the operations of Thompson's construction algorithm.
 * As Brics Automaton uses char for transitions
 */
public class BricsAutomataBuilder implements AutomataBuilder<Automaton, String> {

    private Map<String, Character> labelMappings;
    private Character nextChar;

    public BricsAutomataBuilder() {
        this.labelMappings = Maps.newHashMap();
        this.nextChar = 0;
    }

    @Override
    public Automaton transition(String label) {
        // obtain corresponding character mapping for the label
        Character charMapping = getLabelMapping(label);

        Automaton resultAutomaton = BasicAutomata.makeChar(charMapping);
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

    private Character getLabelMapping(String label) {
        if(labelMappings.containsKey(label)) {
            return  labelMappings.get(label);
        } else {
            Character character = nextChar++;
            labelMappings.put(label, character);
            return character;
        }
    }

}
