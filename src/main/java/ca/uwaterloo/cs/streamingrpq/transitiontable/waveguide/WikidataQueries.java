package ca.uwaterloo.cs.streamingrpq.transitiontable.waveguide;

import ca.uwaterloo.cs.streamingrpq.transitiontable.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.transitiontable.util.PathSemantics;

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

    public static <L> DFA<L> pvdlbq4(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0, 1, predicates[0]);
        query.addDFAEdge(1, 1, predicates[0]);
        query.addDFAEdge(1, 2, predicates[1]);

        query.setStartState(0);
        query.setFinalState(2);
        query.optimize();

        return query;
    }

    public static <L> DFA<L> pvdlbq5(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0,1,predicates[0]);
        query.addDFAEdge(0,1,predicates[1]);
        query.addDFAEdge(0,1,predicates[2]);

        query.setStartState(0);
        query.setFinalState(1);
        query.optimize();

        return query;
    }

    public static <L> DFA<L> pvdlbq21(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0,1,predicates[0]);
        query.addDFAEdge(1,2,predicates[1]);
        query.addDFAEdge(2,1,predicates[0]);

        query.setStartState(0);
        query.setFinalState(2);
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

    public static <L> DFA<L> wwwq3(PathSemantics pathSemantics, int maxSize, L... predicates) {
        DFA<L> query = new DFA<>(maxSize, pathSemantics);

        query.addDFAEdge(0, 1, predicates[0]);
        query.addDFAEdge(1, 1, predicates[1]);
        query.addDFAEdge(1, 2, predicates[2]);
        query.addDFAEdge(2, 2, predicates[2]);

        query.setStartState(0);
        query.setFinalState(1);
        query.setFinalState(2);
        query.optimize();

        return query;
    }


    public static void main(String[] argv) {
        String p0 = "a";
        String p1 = "b";
        String p2 = "c";

        DFA<String> query = WikidataQueries.wwwq3(PathSemantics.SIMPLE, 100, p0, p1, p2);
        query.optimize();

        query.optimize();
    }
}