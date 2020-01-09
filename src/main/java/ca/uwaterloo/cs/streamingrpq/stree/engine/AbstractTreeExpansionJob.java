package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;

import java.util.concurrent.Callable;

public abstract class AbstractTreeExpansionJob<T, N> implements Callable<Integer> {

    /**
     * Populates the job array
     * @param spanningTree
     * @param parentNode
     * @param targetVertex
     * @param targetState
     * @param edgeTimestamp
     * @return false whenever job array is full and cannot be further populated
     */
    abstract  boolean addJob(T spanningTree, N parentNode, int targetVertex, int targetState, long edgeTimestamp) throws IllegalStateException;

    abstract boolean isFull();

    abstract boolean isEmpty();
}
