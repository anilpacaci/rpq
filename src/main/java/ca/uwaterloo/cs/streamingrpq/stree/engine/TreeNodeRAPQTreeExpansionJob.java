package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.Collection;
import java.util.Queue;

public class TreeNodeRAPQTreeExpansionJob<L> extends AbstractTreeExpansionJob<SpanningTreeRAPQ<Integer>, TreeNode<Integer>>{

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

    public TreeNodeRAPQTreeExpansionJob(ProductGraph<Integer,L> productGraph, QueryAutomata<L> automata, Queue<ResultPair<Integer>> results) {
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
            processTransition(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i]);
        }

        return this.resultCount;
    }

    public void processTransition(SpanningTreeRAPQ<Integer> tree, TreeNode<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
        // either update timestamp, or create the node
        if(tree.exists(childVertex, childState)) {
            // if the child node already exists, we might need to update timestamp
            TreeNode childNode = tree.getNode(childVertex, childState);

            // root's children have timestamp equal to the edge timestamp
            // root timestmap always higher than any node in the tree
            if(parentNode.equals(tree.getRootNode())) {
                childNode.setTimestamp(edgeTimestamp);
                parentNode.setTimestamp( edgeTimestamp);
                // properly update the parent pointer
                childNode.setParent(parentNode);
            }
            // child node cannot be the root because parent has to be at least
            else if(childNode.getTimestamp() < Long.min(parentNode.getTimestamp(), edgeTimestamp)) {
                // only update its timestamp if there is a younger  path, back edge is guarenteed to be at smaller or equal
                childNode.setTimestamp(Long.min(parentNode.getTimestamp(), edgeTimestamp));
                // properly update the parent pointer
                childNode.setParent(parentNode);
            }

        } else {
            // extend the spanning tree with incoming node

            // root's children have timestamp equal to the edge timestamp
            // root timestmap always higher than any node in the tree
            TreeNode<Integer> childNode;
            if(parentNode.equals(tree.getRootNode())) {
                childNode = tree.addNode(parentNode, childVertex, childState, edgeTimestamp);
                parentNode.setTimestamp(edgeTimestamp);
            }
            else {
                childNode = tree.addNode(parentNode, childVertex, childState, Long.min(parentNode.getTimestamp(), edgeTimestamp));
            }
            // add this pair to results if it is a final state
            if (automata.isFinalState(childState)) {
                results.offer(new ResultPair<>(tree.getRootVertex(), childVertex));
                resultCount++;
            }

            // get all the forward edges of the new extended node
            Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);

            if (forwardEdges == null) {
                // TODO better nul handling
                // end recursion if node has no forward edges
                return;
            } else {
                // there are forward edges, iterate over them
                for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                    // recursive call as the target of the forwardEdge has not been visited in state targetState before
                    //processTransition(tree, childNode, forwardEdge.getTarget(), targetState, forwardEdge.getTimestamp());
                    processTransition(tree, childNode, forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState(), forwardEdge.getTimestamp());
                }
            }
        }
    }
}
