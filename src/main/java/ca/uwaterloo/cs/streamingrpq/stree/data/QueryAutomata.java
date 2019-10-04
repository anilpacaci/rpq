package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class QueryAutomata<L> {
    public int numOfStates;
    public Set<Integer> finalStates;

    Table<Integer, L, Integer> transitions;
    Table<Integer, L, Set<Integer>> reverseTransitions;

    public QueryAutomata(int numOfStates) {
        finalStates = new HashSet<>();
        transitions = HashBasedTable.create();
        reverseTransitions = HashBasedTable.create();
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
        Set<Integer> sources = getReverseTransitions(target, label);
        sources.add(source);
    }

    public Integer getTransition(int source, L label) {
        return transitions.get(source, label);
    }

    public Set<Integer> getReverseTransitions(int target, L label) {
        Set<Integer> sources;
        if(!reverseTransitions.contains(target, label)) {
            sources = new HashSet<>();
            reverseTransitions.put(target, label, sources);
        } else {
            sources = reverseTransitions.get(target, label);
        }

        return  sources;
    }

    public Map<L, Integer> getTransitions(int source) {
        return transitions.row(source);
    }

    public Map<Integer, Integer> getTransition(L label) {
        return transitions.column(label);
    }

}
