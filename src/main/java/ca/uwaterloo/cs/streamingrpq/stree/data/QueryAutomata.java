package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

public class QueryAutomata<L> {
    public int numOfStates;
    public Set<Integer> finalStates;

    HashMap<Integer, HashMap<L, Integer>> transitions;

    HashMap<Integer, Multimap<L, Integer>> reverseTransitions;

    HashMap<L, HashMap<Integer, Integer>> labelTransitions;

    public QueryAutomata(int numOfStates) {
        finalStates = new HashSet<>();
        transitions =  new HashMap<>();
        reverseTransitions = new HashMap<>();
        labelTransitions = new HashMap<>();
    	this.numOfStates = numOfStates;
    	// initialize transition maps for all
    	for(int i = 0; i < numOfStates; i++) {
    	    transitions.put(i, new HashMap<L, Integer>());
    	    reverseTransitions.put(i, HashMultimap.create());
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
        Multimap<L, Integer> backwardMap = reverseTransitions.get(target);
        backwardMap.put(label, source);
        if(!labelTransitions.containsKey(label)) {
            labelTransitions.put(label, new HashMap<>());
        }
        HashMap<Integer, Integer> labelMap = labelTransitions.get(label);
        labelMap.put(source, target);
    }

    public Integer getTransition(int source, L label) {
        HashMap<L, Integer> forwardMap = transitions.get(source);
        return forwardMap.get(label);
    }

    /**
     *
     * @param target
     * @param label
     * @return an empty collection, not <code>null</null> in case target, label entry does not exists
     */
    public Collection<Integer> getReverseTransitions(int target, L label) {
        Multimap<L, Integer> backwardMap = reverseTransitions.get(target);

        return backwardMap.get(label);
    }

    public Map<Integer, Integer> getTransition(L label) {
        return labelTransitions.getOrDefault(label, new HashMap<>());
    }

}
