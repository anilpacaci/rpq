package ca.uwaterloo.cs.streamingrpq.stree.query;

import java.util.Map;
import java.util.Set;

/**
 * Created by anilpacaci on 2020-01-21.
 */
public abstract class Automata<L> {


    public abstract boolean isFinalState(int state);

    public abstract Map<Integer, Integer> getTransition(L label);

    public abstract  boolean hasContainment(Integer stateQ, Integer stateT);

    public abstract void computeContainmentRelationship();

    public abstract int getNumOfStates();

    public abstract Set<Integer> getFinalStates();

    public abstract Set<L> getAlphabet();

}
