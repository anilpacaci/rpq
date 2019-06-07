package ca.uwaterloo.cs.streamingrpq.waveguide;

import ca.uwaterloo.cs.streamingrpq.dfa.DFA;

public class WaveGuideQueries {

    public static <L> DFA<L> query5(int maxSize, L predicate0, L predicate1, L predicate2) {
        DFA<L> q5 = new DFA<L>(maxSize);
        q5.addDFAEdge(0,1, predicate0);
        q5.addDFAEdge(1,2, predicate1);
        q5.addDFAEdge(2,2, predicate1);
        q5.addDFAEdge(2,3, predicate2);
        q5.addDFAEdge(3,3, predicate2);
        q5.setStartState(0);
        q5.setFinalState(3);
        q5.optimize();

        return q5;
    }

    public static <L> DFA<L> query6(int maxSize, L predicate0, L predicate1, L predicate2) {
        DFA<L> q6 = new DFA<L>(maxSize);
        q6.addDFAEdge(0,1, predicate0);
        q6.addDFAEdge(1,2, predicate1);
        q6.addDFAEdge(2,3, predicate2);
        q6.addDFAEdge(3,1, predicate0);
        q6.setStartState(0);
        q6.setFinalState(3);
        q6.optimize();

        return q6;
    }

    public static <L> DFA<L> restrictedRE(int maxSize, L predicate0, L predicate1) {
        DFA<L> q6 = new DFA<L>(maxSize);
        q6.addDFAEdge(0,0, predicate0);
        q6.addDFAEdge(0,1, predicate1);
        q6.addDFAEdge(1,1, predicate1);
        q6.addDFAEdge(1,2, predicate0);
        q6.addDFAEdge(2,2, predicate0);

        q6.setStartState(0);
        q6.setFinalState(1);
        q6.setFinalState(2);
        q6.optimize();

        return q6;
    }
}
