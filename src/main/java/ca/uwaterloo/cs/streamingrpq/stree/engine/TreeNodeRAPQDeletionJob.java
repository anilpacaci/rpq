package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

public class TreeNodeRAPQDeletionJob<L> extends AbstractTreeExpansionJob<SpanningTreeRAPQ<Integer>, TreeNode<Integer>>{

    private ProductGraph<Integer,L> productGraph;
    private QueryAutomata<L> automata;
    private SpanningTreeRAPQ<Integer> spanningTree[];
    private TreeNode<Integer> parentNode[];
    private int targetVertex[];
    private int targetState[];
    private long edgeTimestamp[];

    private int currentSize;

    private int resultCount;

    private Queue<ResultPair<Integer>> results;

    public TreeNodeRAPQDeletionJob(ProductGraph<Integer,L> productGraph, QueryAutomata<L> automata, Queue<ResultPair<Integer>> results) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.spanningTree = new SpanningTreeRAPQ[Constants.EXPECTED_BATCH_SIZE];
        this.parentNode = new TreeNode[Constants.EXPECTED_BATCH_SIZE];
        this.targetVertex = new int[Constants.EXPECTED_BATCH_SIZE];
        this.targetState = new int[Constants.EXPECTED_BATCH_SIZE];
        this.edgeTimestamp = new long[Constants.EXPECTED_BATCH_SIZE];
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
    public boolean addJob(SpanningTreeRAPQ<Integer> spanningTree, TreeNode<Integer> parentNode, int targetVertex, int targetState, long edgeTimestamp) throws IllegalStateException{
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

    public boolean isFull() {
        return currentSize == Constants.EXPECTED_BATCH_SIZE - 1;
    }

    public boolean isEmpty() {
        return currentSize == 0;
    }

    @Override
    public Integer call() throws Exception {

        // call each job in teh buffer
        for(int i = 0; i < currentSize; i++) {
            markExpired(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i]);
        }

        return this.resultCount;
    }

    public void markExpired(SpanningTreeRAPQ<Integer> tree, TreeNode<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
        // update the timestamp of the entire subtree of such node exists
        if(tree.exists(childVertex, childState)) {
            // if the child node already exists, we might need to update timestamp
            TreeNode<Integer> childNode = tree.getNode(childVertex, childState);

            Queue<TreeNode<Integer>> queue = new ArrayDeque<>();
            queue.offer(childNode);
            while(!queue.isEmpty()) {
                TreeNode currentNode = queue.poll();
                currentNode.setTimestamp(Long.MIN_VALUE);
                queue.addAll(currentNode.getChildren());
            }

            // allnodes are marked,
            // simply call expiry on the spanning tree

            tree.removeOldEdges(edgeTimestamp, productGraph);
            

        } else {
            // there is no such edge so no need for deletion
        }
    }
}
