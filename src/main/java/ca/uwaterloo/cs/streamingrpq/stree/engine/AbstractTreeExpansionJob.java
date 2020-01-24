package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

public abstract class AbstractTreeExpansionJob<L, T extends AbstractSpanningTree<Integer, T, N>, N extends AbstractTreeNode<Integer, T, N>> implements Callable<Integer> {

    protected ProductGraph<Integer,L> productGraph;
    protected Automata<L> automata;
    protected T spanningTree[];
    protected N parentNode[];
    protected int targetVertex[];
    protected int targetState[];
    protected long edgeTimestamp[];
    protected boolean isDeletion;

    protected int currentSize;

    protected int resultCount;

    protected Set<ResultPair<Integer>> results;

    protected AbstractTreeExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results, boolean isDeletion) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.targetVertex = new int[Constants.EXPECTED_BATCH_SIZE];
        this.targetState = new int[Constants.EXPECTED_BATCH_SIZE];
        this.edgeTimestamp = new long[Constants.EXPECTED_BATCH_SIZE];
        this.isDeletion = isDeletion;
        this.results = results;
        this.currentSize = 0;
        this.resultCount = 0;
    }

    /**
     * Populates the job array
     * @param spanningTree
     * @param parentNode
     * @param targetVertex
     * @param targetState
     * @param edgeTimestamp
     * @return false whenever job array is full and cannot be further populated
     */
    public  boolean addJob(T spanningTree, N parentNode, int targetVertex, int targetState, long edgeTimestamp) throws IllegalStateException {
        if(this.currentSize >= Constants.EXPECTED_BATCH_SIZE) {
            throw new IllegalStateException("Job capacity exceed limit " + currentSize);
        }

        this.spanningTree[currentSize] = spanningTree;
        this.parentNode[currentSize] = parentNode;
        this.targetVertex[currentSize] = targetVertex;
        this.targetState[currentSize] = targetState;
        this.edgeTimestamp[currentSize] = edgeTimestamp;
        this.currentSize++;

        if(currentSize == Constants.EXPECTED_BATCH_SIZE - 1) {
            return false;
        }

        return true;
    }

    /**
     * Determines whether the current batch is full
     * @return
     */
    public boolean isFull() {
        return currentSize == Constants.EXPECTED_BATCH_SIZE - 1;
    }

    /**
     * Determines whether the current batch is empty
     * @return
     */
    public boolean isEmpty() {
        return currentSize == 0;
    }

    /**
     * Process a single transition (edge) of the product graph over the spanning tree
     * @param tree SpanningTree where the transition will be processed over
     * @param parentNode SpanningTree node as the source of the transition
     * @param childVertex
     * @param childState
     * @param edgeTimestamp
     */
    public abstract void processTransition(T tree, N parentNode, int childVertex, int childState, long edgeTimestamp);

    /**
     * Explicit deletion processing.
     * Mark all the nodes in the subtree of the deleted edge with an expired timestamp;
     * call window expiry procedure
     * @param tree
     * @param parentNode
     * @param childVertex
     * @param childState
     * @param timestamp
     */
    public abstract void markExpired(T tree, N parentNode, int childVertex, int childState, long timestamp);

    @Override
    public abstract Integer call() throws Exception;
}
