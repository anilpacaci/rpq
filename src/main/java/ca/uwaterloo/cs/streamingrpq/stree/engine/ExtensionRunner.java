package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ExtensionRunner<L> implements Callable<Multimap<Integer, Integer>> {

    private final Graph<Integer,L> graph;
    private final Delta<Integer> delta;
    private final QueryAutomata<L> automata;

    private SpanningTree<Integer> tree;
    int parentVertex;
    int parentState;
    int childVertex;
    int childState;
    long edgeTimestamp;

    Multimap<Integer, Integer> results;

    public ExtensionRunner(Graph<Integer, L> graph, Delta<Integer> delta, QueryAutomata<L> automata, SpanningTree<Integer> tree, int parentVertex, int parentState, int childVertex, int childState, long edgeTimestamp) {
        this.graph = graph;
        this.delta = delta;
        this.automata  = automata;

        this.tree = tree;
        this.parentVertex = parentVertex;
        this.parentState = parentState;
        this.childVertex = childVertex;
        this.childState = childState;
        this.edgeTimestamp = edgeTimestamp;

        results = HashMultimap.create();
    }

    @Override
    public Multimap<Integer, Integer> call() throws Exception {
        processTransition(this.tree, this.parentVertex, this.parentState, this.childVertex, this.childState, this.edgeTimestamp);
        return results;
    }

    public void processTransition(SpanningTree<Integer> tree, int parentVertex, int parentState, int childVertex, int childState, long edgeTimestamp) {
        TreeNode parentNode = tree.getNode(parentVertex, parentState);

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
            if(parentNode.equals(tree.getRootNode())) {
                tree.addNode(parentNode, childVertex, childState, edgeTimestamp);
                parentNode.setTimestamp(edgeTimestamp);
            }
            else {
                tree.addNode(parentNode, childVertex, childState, Long.min(parentNode.getTimestamp(), edgeTimestamp));
            }
            // add this pair to results if it is a final state
            if (automata.isFinalState(childState)) {
                results.put(tree.getRootVertex(), childVertex);
            }

            // get all the forward edges of the new extended node
            Collection<GraphEdge<Integer, L>> forwardEdges = graph.getForwardEdges(childVertex);

            if (forwardEdges == null) {
                // TODO better nul handling
                // end recursion if node has no forward edges
                return;
            } else {
                // there are forward edges, iterate over them
                for (GraphEdge<Integer, L> forwardEdge : forwardEdges) {
                    Integer targetState = automata.getTransition(childState, forwardEdge.getLabel());
                    // no need to check if the target node exists as we might need to update its timestamp even if it exists
                    if (targetState != null) {
                        // recursive call as the target of the forwardEdge has not been visited in state targetState before
                        processTransition(tree, childVertex, childState, forwardEdge.getTarget(), targetState, forwardEdge.getTimestamp());
                    }
                }
            }
        }
    }
}
