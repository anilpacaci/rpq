package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.SpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.TreeNodeRSPQ;

import java.util.concurrent.Callable;

public abstract class AbstractTreeExpansionJob implements Callable<Integer> {

    /**
     * Populates the job array
     * @param spanningTree
     * @param parentNode
     * @param targetVertex
     * @param targetState
     * @param edgeTimestamp
     * @return false whenever job array is full and cannot be further populated
     */
    abstract <T extends SpanningTree, N extends TreeNode> boolean addJob(T spanningTree, N parentNode, int targetVertex, int targetState, long edgeTimestamp) throws IllegalStateException;

    abstract boolean isFull();

    abstract boolean isEmpty();
}
