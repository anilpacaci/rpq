package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.googlecode.cqengine.query.simple.In;

import java.util.Map;

public class IncrementalRAPQ {

    private Delta<Integer> delta;
    private Graph<Integer, String> graph;
    private QueryAutomata<String> automata;

    private Multimap<Integer, Integer> results;

    public IncrementalRAPQ(QueryAutomata<String> query) {
        delta = new Delta<>();
        automata = query;
        results = HashMultimap.create();
        graph = new Graph<>();
    }

    public void processTransition(SpanningTree<Integer> tree,  int parentVertex, int parentState, int childVertex, int childState) {
        TreeNode parentNode = tree.getNode(parentVertex, parentState);
        if(!tree.exists(childVertex, childState)) {
            // extend the spanning tree with incoming noe
            tree.addNode(parentNode, childVertex, childState, parentNode.getTimestamp());

            // add this pair to results if it is a final state
            if(automata.isFinalState(childState)) {
                results.put(tree.getRootVertex(), childVertex);
            }

            // get all the forward edges of the new extended node
            Multimap<String, Integer> forwardEdges = graph.getForwardEdges(childVertex);

            if(forwardEdges == null) {
                // TODO better nul handling
                // end recursion if node has no forward edges
                return;
            }

            for(Map.Entry<String, Integer> forwardEdge : forwardEdges.entries()) {
                Integer targetState = automata.getTransition(childState, forwardEdge.getKey());
                if(targetState != null && !tree.exists(forwardEdge.getValue(), targetState)) {
                    // recursive call as the target of the forwardEdge has not been visited in state targetState before
                    processTransition(tree, childVertex, childState, forwardEdge.getValue(), targetState);
                }
            }
        }
    }

    public void processEdge(InputTuple<Integer, Integer, String> inputTuple) {
        // add edge to the snapshot graph
        graph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel());

        // retrieve all transition that can be performed with this label
        Map<Integer, Integer> transitions = automata.getTransition(inputTuple.getLabel());

        //create a spanning tree for the source node in case it does not exists
        if(transitions.keySet().contains(0) && !delta.exists(inputTuple.getSource())) {
            // if there exists a start transition with given label, there should be a spanning tree rooted at source vertex
            delta.addTree(inputTuple.getSource());
        }

        // for each spanning tree in Delta
        for(SpanningTree spanningTree : delta.getTrees()) {
            // for each transition that given label satisy
            for(Map.Entry<Integer, Integer> transition : transitions.entrySet()) {
                // if the source already exists, but not the target
                if(spanningTree.exists(inputTuple.getSource(), transition.getKey()) && !spanningTree.exists(inputTuple.getTarget(), transition.getValue())) {
                    processTransition(spanningTree, inputTuple.getSource(), transition.getKey(), inputTuple.getTarget(), transition.getValue());
                }
            }
        }
    }

    public Multimap<Integer, Integer> getResults() {
        return  results;
    }
}
