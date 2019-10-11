package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.concurrent.Callable;

public class TreeExpansionJob<L> implements Callable<Multimap<Integer, Integer>>{

    private ProductGraph<Integer,L> productGraph;
    private QueryAutomata<L> automata;
    private SpanningTree<Integer> spanningTree;
    private TreeNode<Integer> parentNode;
    private int targetVertex;
    private int targetState;
    private long edgeTimestamp;

    private Multimap<Integer, Integer> results;

    public TreeExpansionJob(ProductGraph<Integer,L> productGraph, QueryAutomata<L> automata, SpanningTree<Integer> spanningTree, TreeNode<Integer> parentNode, int targetVertex, int targetState, long edgeTimestamp) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.spanningTree = spanningTree;
        this.parentNode = parentNode;
        this.targetVertex = targetVertex;
        this.targetState = targetState;
        this.edgeTimestamp = edgeTimestamp;
        this.results = HashMultimap.create();
    }

    @Override
    public Multimap<Integer, Integer> call() throws Exception {
        processTransition(spanningTree, parentNode, targetVertex, targetState, edgeTimestamp);

        return results;
    }

    public void processTransition(SpanningTree<Integer> tree, TreeNode<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
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
                results.put(tree.getRootVertex(), childVertex);
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
