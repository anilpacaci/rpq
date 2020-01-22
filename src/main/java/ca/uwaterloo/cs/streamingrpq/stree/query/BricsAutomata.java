package ca.uwaterloo.cs.streamingrpq.stree.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import dk.brics.automaton.*;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class BricsAutomata extends Automata<String> {

    private Map<Character, String> reverseLabelMappings;
    private Map<String, Character> labelMappings;

    private Automaton automaton;

    public BricsAutomata(Automaton automaton, Map<String, Character> labelMappings) {
        super();
        this.labelMappings = labelMappings;
        reverseLabelMappings = Maps.newHashMap();
        // populate reverse label mappings
        for(Map.Entry<String, Character> label : labelMappings.entrySet()) {
            reverseLabelMappings.put(label.getValue(), label.getKey());
        }
        this.automaton = automaton;
    }

    public void finalize() {
        // determinize and minimizes the given automaton
        automaton.minimize();

        // perform a breadth first traversal of the transition graph in order to record all transitions
        // data structures for simple BFS traversal
        Set<State> visited = Sets.newHashSet();
        Queue<State> queue = Lists.newLinkedList();

        queue.offer(this.automaton.getInitialState());

        while(!queue.isEmpty()) {
            State state = queue.poll();
            visited.add(state);
            if(state.isAccept()) {
                addFinalState(getStateNumber(state));
            }

            // go over all transitions of the current state
            // since Automaton is deterministic, no need to iterate over epsilon edges
            for(Transition transition : state.getTransitions()) {
                State targetState = transition.getDest();
                // we only have character transitions, so retrieve min
                String label = reverseLabelMappings.get(transition.getMax());

                // retrieve or create mapping for this specific label
                Map<Integer, Integer> transitions = labelTransitions.computeIfAbsent(label, key -> Maps.newHashMap());

                transitions.put(getStateNumber(state), getStateNumber(targetState));
                // add target state to queue if it is not visited before
                if(!visited.contains(targetState)) {
                    queue.offer(targetState);
                }
            }
        }
        // now automaton states are assigned to a contigious range, and transitions are grouped by label
        // finally compute the containment relationship
        computeContainmentRelationship();
    }

}
