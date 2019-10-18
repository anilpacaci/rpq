package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.SpanningTreeRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.TreeNodeRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.Collection;
import java.util.Queue;
import java.util.Stack;

public class TreeNodeRSPQTreeExpansionJob<L> extends AbstractTreeExpansionJob<SpanningTreeRSPQ<Integer>, TreeNodeRSPQ<Integer>>{

    private ProductGraph<Integer,L> productGraph;
    private QueryAutomata<L> automata;
    private SpanningTreeRSPQ<Integer> spanningTree[];
    private TreeNodeRSPQ<Integer> parentNode[];
    private int targetVertex[];
    private int targetState[];
    private long edgeTimestamp[];

    private int currentSize;

    private int resultCount;

    private Queue<ResultPair<Integer>> results;

    public TreeNodeRSPQTreeExpansionJob(ProductGraph<Integer,L> productGraph, QueryAutomata<L> automata, Queue<ResultPair<Integer>> results) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.spanningTree = new SpanningTreeRSPQ[Constants.EXPECTED_BATCH_SIZE];
        this.parentNode = new TreeNodeRSPQ[Constants.EXPECTED_BATCH_SIZE];
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
    public boolean addJob(SpanningTreeRSPQ<Integer> spanningTree, TreeNodeRSPQ<Integer> parentNode, int targetVertex, int targetState, long edgeTimestamp) throws IllegalStateException{
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
            if(!parentNode[i].containsCM(targetVertex[i], targetState[i]) && !spanningTree[i].isMarked(targetVertex[i], targetState[i])) {
                processTransition(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i]);
            }
        }

        return this.resultCount;
    }

    public void processTransition(SpanningTreeRSPQ<Integer> tree, TreeNodeRSPQ<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
        if(!automata.hasContainment(parentNode.getFirstCM(childVertex), childState)) {
            // detected conflict parent node needs to be unmarked
            unmark(tree, parentNode);
        } else {
            // if this is the first target product node is visited, simply add it to the markings
            if(!tree.exists(childVertex, childState)) {
                tree.addMarking(childVertex, childState);
            }

            //add new node to the tree as a new child
            TreeNodeRSPQ<Integer> childNode;
            if(parentNode.equals(tree.getRootNode())) {
                childNode = tree.addNode(parentNode, childVertex, childState, edgeTimestamp);
                parentNode.setTimestamp(edgeTimestamp);
            } else {
                childNode = tree.addNode(parentNode, childVertex, childState, Long.min(parentNode.getTimestamp(), edgeTimestamp));
            }

            if(automata.isFinalState(childState)) {
                results.offer(new ResultPair<>(tree.getRootVertex(), childVertex));
                resultCount++;
            }

            // get all the forward edges of the new extended node
            Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);

            if(forwardEdges == null) {
                return;
            } else {
                // there are forward edges, iterate over them
                for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                    int targetVertex = forwardEdge.getTarget().getVertex();
                    int targetState = forwardEdge.getTarget().getState();
                    if(!childNode.containsCM(targetVertex, targetState) && !tree.isMarked(targetVertex, targetState)) {
                        // visit a node only if that same node is not visited at the same state before
                        // simply prevent cycles in product graph
                        processTransition(tree, childNode, targetVertex, targetState, forwardEdge.getTimestamp());
                    }
                }
            }
        }
    }

    /**
     * Implements unmarking procedure for nodes with conflicts
     * @param tree
     * @param parentNode
     */
    public void unmark(SpanningTreeRSPQ<Integer> tree, TreeNodeRSPQ<Integer> parentNode) {

        Stack<TreeNodeRSPQ<Integer>> unmarkedNodes = new Stack<>();

        TreeNodeRSPQ<Integer> currentNode = parentNode;

        // first unmark all marked nodes upto parent
        while(currentNode != null && tree.isMarked(currentNode.getVertex(), currentNode.getState())) {
            tree.removeMarking(currentNode.getVertex(), currentNode.getState());
            unmarkedNodes.push(currentNode);
            currentNode = currentNode.getParent();
        }

        // now simply traverse back edges of the candidates and invoke processTransition
        while(!unmarkedNodes.isEmpty()) {
            currentNode = unmarkedNodes.pop();

            // get backward edges of the unmarked node
            Collection<GraphEdge<ProductGraphNode<Integer>>> backwardEdges = productGraph.getBackwardEdges(currentNode.getVertex(), currentNode.getState());
            for(GraphEdge<ProductGraphNode<Integer>> backwardEdge : backwardEdges) {
                int sourceVertex = backwardEdge.getSource().getVertex();
                int sourceState = backwardEdge.getSource().getState();
                // find all the nodes that are pruned due to previously marking
                Collection<TreeNodeRSPQ<Integer>> parentNodes = tree.getNodes(sourceVertex, sourceState);
                for(TreeNodeRSPQ<Integer> parent : parentNodes) {
                    // try to extend if it is not a cycle in the product graph
                    if(!parent.containsCM(currentNode.getVertex(), currentNode.getState())) {
                        processTransition(tree, parent, currentNode.getVertex(), currentNode.getState(), backwardEdge.getTimestamp());
                    }
                }
            }
        }

    }
}
