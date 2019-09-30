package ca.uwaterloo.cs.streamingrpq.waveguide;

import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.util.PathSemantics;

public class WaveGuideQueries {

    public static <L> DFA<L> query5(PathSemantics pathSemantics, int maxSize, L ... predicates) {
        DFA<L> q5 = new DFA<L>(maxSize, pathSemantics);
        q5.addDFAEdge(0,1, predicates[0]);
        q5.addDFAEdge(1,2, predicates[1]);
        q5.addDFAEdge(2,2, predicates[1]);
        q5.addDFAEdge(2,3, predicates[2]);
        q5.addDFAEdge(3,3, predicates[2]);
        q5.setStartState(0);
        q5.setFinalState(3);
        q5.optimize();

        return q5;
    }

    public static <L> DFA<L> query6(PathSemantics pathSemantics, int maxSize, L ... predicates) {
        DFA<L> q6 = new DFA<L>(maxSize, pathSemantics);
        q6.addDFAEdge(0,1, predicates[0]);
        q6.addDFAEdge(1,2, predicates[1]);
        q6.addDFAEdge(2,3, predicates[2]);
        q6.addDFAEdge(3,1, predicates[0]);
        q6.setStartState(0);
        q6.setFinalState(3);
        q6.optimize();

        return q6;
    }

    public static <L> DFA<L> restrictedRE(PathSemantics pathSemantics, int maxSize, L ... predicates) {
        DFA<L> q6 = new DFA<L>(maxSize, pathSemantics);
        q6.addDFAEdge(0,0, predicates[0]);
        q6.addDFAEdge(0,1, predicates[1]);
        q6.addDFAEdge(1,1, predicates[1]);
        q6.addDFAEdge(1,2, predicates[0]);
        q6.addDFAEdge(2,2, predicates[0]);

        q6.setStartState(0);
        q6.setFinalState(0);
        q6.setFinalState(1);
        q6.setFinalState(2);
        q6.optimize();

        return q6;
    }

    public static void main(String[] argv) {
        String p0 = "a";
        String p1 = "b";
        String p2 = "c";

        DFA<String> query = WaveGuideQueries.query6(PathSemantics.SIMPLE, 100, p0, p1, p2);
        query.optimize();

        query.optimize();
    }

}
