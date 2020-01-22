package ca.uwaterloo.cs.streamingrpq.stree.query;

/**
 * Created by anilpacaci on 2020-01-21.
 */
public interface AutomataBuilder<A, C> {
    A transition(C label);

    A kleeneStar(A nfa);

    A concenetation(A first, A second);

    A alternation(A first, A second);
}
