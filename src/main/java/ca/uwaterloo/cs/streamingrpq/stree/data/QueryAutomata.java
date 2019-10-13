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

    private boolean containmentMark[][];

    public QueryAutomata(int numOfStates) {
        this.containmentMark = new boolean[numOfStates][numOfStates];
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

    public boolean hasContainment(Integer stateQ, Integer stateT) {
        if(stateQ == null) {
            return true;
        }
        return !this.containmentMark[stateQ][stateT];
    }

    /**
     * Optimization procedure for the autamaton, including minimization, containment relationship
     * MUST be called after automaton is constructed
     */
    public void computeContainmentRelationship() {
        int alphabetSize = labelTransitions.keySet().size();

        // once we construct the minimized DFA, we can easily compute the sufflix language containment relationship
        // Algorithm S of Wood'95
        ArrayList[][] statePairMatrix = new ArrayList[this.numOfStates][this.numOfStates];
        for(int s = 0; s < this.numOfStates; s++) {
            for (int t = 0; t < this.numOfStates; t++) {
                statePairMatrix[s][t] = new ArrayList<>();
            }
        }
        // first create a transition matrix for the DFA
        int[][] transitionMatrix = new int[this.numOfStates][alphabetSize];
        for(int i = 0; i < this.numOfStates; i++) {
            for(int j = 0; j < alphabetSize; j++) {
                transitionMatrix[i][j] = -1;
            }
        }

        Iterator<L> edgeIterator = labelTransitions.keySet().iterator();;
        for(int j = 0 ; j < alphabetSize; j++) {
            HashMap<Integer, Integer> edges = labelTransitions.get(edgeIterator.next());
            for (Map.Entry<Integer, Integer> edge : edges.entrySet()) {
                transitionMatrix[edge.getKey()][j] = edge.getValue();
            }
        }

        // initialize: line 1 of Algorithm S
        for(int s = 0; s < this.numOfStates; s++) {
            for (int t = 0; t < this.numOfStates; t++) {
                // for s \in S-F and t \in F
                if(!finalStates.contains(s) && finalStates.contains(t)) {
                    containmentMark[s][t] = true;
                }
            }
        }

        // line 2-7 of Algorithm S0
        for(int s = 0; s < this.numOfStates; s++) {
            for (int t = 0; t < this.numOfStates; t++) {
                // for s,t \in ((SxS) - ((S-F)xF))
                if(finalStates.contains(s) || !finalStates.contains(t)) {
                    // implement line 3,
                    boolean isMarked = false;
                    Queue<StatePair> markQueue = new ArrayDeque<>();
                    for(int j = 0; j < alphabetSize; j++) {
                        if(transitionMatrix[s][j] == transitionMatrix[t][j] && transitionMatrix[s][j] != -1) {
                            isMarked = true;
                            markQueue.add(StatePair.createInstance(s,t));
                        }
                    }

                    // recursively mark all the pairs on the list of pairs that are marked in this step
                    // line 5 of the Algorithm S
                    while(!markQueue.isEmpty()) {
                        StatePair pair = markQueue.poll();
                        List<StatePair> pairList = statePairMatrix[pair.stateS][pair.stateT];
                        for(StatePair candidate : pairList) {
                            if(!containmentMark[candidate.stateS][candidate.stateT]) {
                                markQueue.add(candidate);
                                containmentMark[candidate.stateS][candidate.stateT] = true;
                            }
                        }
                    }

                    // if there is no marked, then populate the lists
                    // line 6 of Algorithm S
                    if(!isMarked) {
                        for(int j = 0; j < alphabetSize; j++) {
                            int sEndpoint = transitionMatrix[s][j];
                            int tEndpoint = transitionMatrix[t][j];
                            if(sEndpoint != -1 && tEndpoint != -1 && sEndpoint != tEndpoint) {
                                // Line 7 of Algorithm S
                                statePairMatrix[sEndpoint][tEndpoint].add(StatePair.createInstance(s,t));
                            }
                        }
                    }

                }
            }
        }

    }

    private static class StatePair {
        public final int stateS, stateT;

        private StatePair(int stateS, int  stateT) {
            this.stateS = stateS;
            this.stateT = stateT;
        }

        public static StatePair createInstance(int stateS, int stateT) {
            return new StatePair(stateS,stateT);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof StatePair)) {
                return false;
            }

            StatePair sp = (StatePair) o;
            return this.stateS == sp.stateS && this.stateT == sp.stateT;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + this.stateS;
            result = 31 * result + this.stateT;
            return result;
        }

        @Override
        public String toString() {
            return new StringBuilder().
                    append("<").append(stateS).append(",").append(stateT)
                    .append(">").toString();
        }
    }

}
