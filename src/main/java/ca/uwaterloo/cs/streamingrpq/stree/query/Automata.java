package ca.uwaterloo.cs.streamingrpq.stree.query;

import ca.uwaterloo.cs.streamingrpq.stree.data.QueryAutomata;
import com.google.common.collect.Maps;

import java.util.*;

/**
 * Created by anilpacaci on 2020-01-21.
 */
public abstract class Automata<L> {

    private boolean containmentMark[][];

    protected HashMap<L, HashMap<Integer, Integer>> labelTransitions;

    protected Automata() {
        labelTransitions = Maps.newHashMap();
    }

    public abstract boolean isFinalState(int state);

    public abstract Map<Integer, Integer> getTransition(L label);

    public abstract int getNumOfStates();

    public abstract Set<Integer> getFinalStates();

    public abstract Set<L> getAlphabet();

    public boolean hasContainment(Integer stateQ, Integer stateT) {
        if(stateQ == null) {
            return true;
        }
        return !this.containmentMark[stateQ][stateT];
    }

    public void computeContainmentRelationship() {
        int alphabetSize = getAlphabet().size();
        int numOfStates = getNumOfStates();

        // initialize the containmentMark matrix
        this.containmentMark = new boolean[numOfStates][numOfStates];

        // once we construct the minimized DFA, we can easily compute the sufflix language containment relationship
        // Algorithm S of Wood'95
        ArrayList[][] statePairMatrix = new ArrayList[numOfStates][numOfStates];
        for(int s = 0; s < numOfStates; s++) {
            for (int t = 0; t < numOfStates; t++) {
                statePairMatrix[s][t] = new ArrayList<>();
            }
        }
        // first create a transition matrix for the DFA
        int[][] transitionMatrix = new int[numOfStates][alphabetSize];
        for(int i = 0; i < numOfStates; i++) {
            for(int j = 0; j < alphabetSize; j++) {
                transitionMatrix[i][j] = -1;
            }
        }

        Iterator<L> edgeIterator = getAlphabet().iterator();;
        for(int j = 0 ; j < alphabetSize; j++) {
            Map<Integer, Integer> edges = getTransition(edgeIterator.next());
            for (Map.Entry<Integer, Integer> edge : edges.entrySet()) {
                transitionMatrix[edge.getKey()][j] = edge.getValue();
            }
        }

        // initialize: line 1 of Algorithm S
        for(int s = 0; s < numOfStates; s++) {
            for (int t = 0; t < numOfStates; t++) {
                // for s \in S-F and t \in F
                if(!isFinalState(s) && isFinalState(t)) {
                    containmentMark[s][t] = true;
                }
            }
        }

        // line 2-7 of Algorithm S0
        for(int s = 0; s < numOfStates; s++) {
            for (int t = 0; t < numOfStates; t++) {
                // for s,t \in ((SxS) - ((S-F)xF))
                if(isFinalState(s) || !isFinalState(t)) {
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
