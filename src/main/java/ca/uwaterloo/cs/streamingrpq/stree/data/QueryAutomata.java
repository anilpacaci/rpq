package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.*;

public class QueryAutomata<L> {
    public int numOfStates;
    public Set<Integer> finalStates;

    Table<Integer, L, Integer> transitions;

    public QueryAutomata(int numOfStates) {
        finalStates = new HashSet<>();
        transitions = HashBasedTable.create();
    	this.numOfStates = numOfStates;
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
        transitions.put(source, label, target);
    }

    public Integer getTransition(int source, L label) {
        return transitions.get(source, label);
    }

    public Map<L, Integer> getTransitions(int source) {
        return transitions.row(source);
    }

    public Map<Integer, Integer> getTransition(L label) {
        return transitions.column(label);
    }

}
