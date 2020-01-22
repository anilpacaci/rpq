package ca.uwaterloo.cs.streamingrpq.stree.query;

import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class NFA extends Automata<String> {

    private State<String> entry;
    private State<String> exit;

    private boolean isFinalized;
    private int stateCounter;
    private Map<State, Integer> stateNumberMapping;

    public NFA() {
        super();
        entry = new State<>();
        exit = new State<>();
        isFinalized = false;

        stateCounter = 0;
        this.stateNumberMapping = Maps.newHashMap();
    }

    public NFA(State<String> entry, State<String>  exit) {
        this();
        this.entry = entry;
        this.exit = exit;
    }

    public State<String> getEntry() {
        return entry;
    }

    public State<String> getExit() {
        return exit;
    }

    /**
     * Checks whether this automata has been finalized. Finalized automatas cannot perform any further changes
     * @return
     */
    public boolean isFinalized() {
        return isFinalized;
    }

    /**
     * Minimize this automata and generate transition functions. This automata cannot be altered once it is finalizes.
     */
    public void finalize() {
        this.isFinalized = true;

        Set<State<String>> states = Sets.newHashSet();
        Set<String> alphabet = Sets.newHashSet();

        // data structure to store transitions
        labelTransitions.put(Constants.EPSILON_TRANSITION, Maps.newHashMap());

        //data structures for BFS traversal of the transition graph
        Set<State<String>> visited = Sets.newHashSet();
        Queue<State<String>> queue = Lists.newLinkedList();

        queue.offer(entry);

        while(!queue.isEmpty()) {
            State<String> state = queue.poll();
            visited.add(state);

            // go over all outgoing edges with a labels
            for(String label : state.getTransitions().keySet()) {
                Collection<State> targetStates = state.getTransitions(label);
                Map<Integer, Integer> transitions = labelTransitions.computeIfAbsent(label, key -> Maps.newHashMap());

                for(State targetState : targetStates) {
                    transitions.put(getStateNumber(state), getStateNumber(targetState));
                    // add to queue if the state is not visited before
                    if(!visited.contains(targetState)) {
                        queue.offer(targetState);
                    }
                }
            }

            // go over all epsilon transitions
            Map<Integer, Integer> transitions = labelTransitions.get(Constants.EPSILON_TRANSITION);
            for(State targetState : state.getEmptyTransitions()) {
                transitions.put(getStateNumber(state), getStateNumber(targetState));
                // add to queue if the state is not visited before
                if(!visited.contains(targetState)) {
                    queue.offer(targetState);
                }
            }

        }

    }

    /**
     * Assign each state to a consecutive range in [0..n-1]
     * @param state
     * @return
     */
    private int getStateNumber(State<String> state) {
        if(stateNumberMapping.containsKey(state)) {
            return stateNumberMapping.get(state);
        } else {
            int stateNumber = stateCounter++;
            stateNumberMapping.put(state, stateNumber);
            return  stateNumber;
        }
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
