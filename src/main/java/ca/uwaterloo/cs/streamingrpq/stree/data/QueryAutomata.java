package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

public class QueryAutomata<L> extends Automata<L> {

    private int numOfStates;
    private Set<Integer> finalStates;

    private HashMap<Integer, HashMap<L, Integer>> transitions;

    private boolean containmentMark[][];

    public QueryAutomata(int numOfStates) {
        super();
        this.containmentMark = new boolean[numOfStates][numOfStates];
        finalStates = new HashSet<>();
        transitions =  new HashMap<>();
    	this.numOfStates = numOfStates;
    	// initialize transition maps for all
    	for(int i = 0; i < numOfStates; i++) {
    	    transitions.put(i, new HashMap<L, Integer>());
        }
	}

    public void addFinalState(int state) {
        if(state >= numOfStates) {
            // TODO invalid final state
        }
        finalStates.add(state);
    }

    public boolean isFinalState(int state) {
        return finalStates.contains(state);
    }

    public void addTransition(int source, L label, int target) {
        HashMap<L, Integer> forwardMap = transitions.get(source);
        forwardMap.put(label, target);
        if(!labelTransitions.containsKey(label)) {
            labelTransitions.put(label, new HashMap<>());
        }
        HashMap<Integer, Integer> labelMap = labelTransitions.get(label);
        labelMap.put(source, target);
    }


    public Map<Integer, Integer> getTransition(L label) {
        return labelTransitions.getOrDefault(label, new HashMap<>());
    }

    @Override
    public int getNumOfStates() {
        return this.numOfStates;
    }

    @Override
    public Set<Integer> getFinalStates() {
        return this.finalStates;
    }

    @Override
    public Set<L> getAlphabet() {
        return this.labelTransitions.keySet();
    }

}
