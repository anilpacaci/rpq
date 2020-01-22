package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

public class ManualQueryAutomata<L> extends Automata<L> {

    private HashMap<Integer, HashMap<L, Integer>> transitions;

    // overwrite the private field in super class
    private int numOfStates;

    public ManualQueryAutomata(int numOfStates) {
        super();
        this.containmentMark = new boolean[numOfStates][numOfStates];
        transitions =  new HashMap<>();
    	this.numOfStates = numOfStates;
    	// initialize transition maps for all
    	for(int i = 0; i < numOfStates; i++) {
    	    transitions.put(i, new HashMap<L, Integer>());
        }
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

    @Override
    public void finalize() {
        // only thing to be performed is to compute the containment relationship
        computeContainmentRelationship();
    }
}
