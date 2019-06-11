package ca.uwaterloo.cs.streamingrpq.waveguide;

import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.util.PathSemantics;

public class WikidataQueries {

    public static <L> DFA<L> pvdlbq1(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0,1,predicates[0]);
        query.addDFAEdge(0,1,predicates[1]);
        query.addDFAEdge(0,1,predicates[2]);
        query.addDFAEdge(1,1,predicates[0]);
        query.addDFAEdge(1,1,predicates[1]);
        query.addDFAEdge(1,1,predicates[2]);
        query.setStartState(0);
        query.setFinalState(1);
        query.optimize();

        return query;
    }

    public static <L> DFA<L> pvdlbq2(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0,1,predicates[0]);

        query.addDFAEdge(1,1,predicates[0]);

        query.setStartState(0);
        query.setFinalState(1);
        query.optimize();

        return query;
    }

    public static <L> DFA<L> pvdlbq3(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0, 1, predicates[0]);
        query.addDFAEdge(1, 2, predicates[1]);
        query.addDFAEdge(2, 3, predicates[2]);

        query.setStartState(0);
        query.setFinalState(3);
        query.optimize();

        return query;
    }

    public static <L> DFA<L> wwwq2(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0, 1, predicates[0]);
        query.addDFAEdge(1, 1, predicates[1]);

        query.setStartState(0);
        query.setFinalState(1);
        query.optimize();

        return query;
    }
}