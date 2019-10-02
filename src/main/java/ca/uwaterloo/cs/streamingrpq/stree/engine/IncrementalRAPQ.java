package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import com.codahale.metrics.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IncrementalRAPQ<L> extends RPQEngine<L> {

    public IncrementalRAPQ(QueryAutomata<L> query, int capacity) {
        super(query, capacity);
    }

    @Override
    public void processTransition(SpanningTree<Integer> tree,  int parentVertex, int parentState, int childVertex, int childState, long edgeTimestamp) {
        TreeNode parentNode = tree.getNode(parentVertex, parentState);
        // extend the spanning tree with incoming noe
        tree.addNode(parentNode, childVertex, childState, parentNode.getTimestamp());

        // add this pair to results if it is a final state
        if(automata.isFinalState(childState)) {
            results.put(tree.getRootVertex(), childVertex);
        }

        // get all the forward edges of the new extended node
        Collection<GraphEdge<Integer, L>> forwardEdges = graph.getForwardEdges(childVertex);

        if(forwardEdges == null) {
            // TODO better nul handling
            // end recursion if node has no forward edges
            return;
        }

        for(GraphEdge<Integer, L> forwardEdge : forwardEdges) {
            Integer targetState = automata.getTransition(childState, forwardEdge.getLabel());
            if(targetState != null && !tree.exists(forwardEdge.getTarget(), targetState)) {
                // recursive call as the target of the forwardEdge has not been visited in state targetState before
                processTransition(tree, childVertex, childState, forwardEdge.getTarget(), targetState, 0);
            }
        }
    }

    @Override
    public void processEdge(InputTuple<Integer, Integer, L> inputTuple) {
        Long startTime = System.nanoTime();
        Timer.Context timer = fullTimer.time();

        // retrieve all transition that can be performed with this label
        Map<Integer, Integer> transitions = automata.getTransition(inputTuple.getLabel());

        if(transitions.isEmpty()) {
            // there is no transition with given label, simply return
            return;
        } else {
            // add edge to the snapshot graph
            graph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), 0);
        }

        //create a spanning tree for the source node in case it does not exists
        if(transitions.keySet().contains(0) && !delta.exists(inputTuple.getSource())) {
            // if there exists a start transition with given label, there should be a spanning tree rooted at source vertex
            // root node of this created tree has the timestamp of the inserted edge
            delta.addTree(inputTuple.getSource(), inputTuple.getTimestamp());
        }

        List<Map.Entry<Integer, Integer>> transitionList = transitions.entrySet().stream().collect(Collectors.toList());

        // for each transition that given label satisy
        for(Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();

            // iterate over spanning trees that include the source node
            for(SpanningTree spanningTree : delta.getTrees(inputTuple.getSource(), sourceState)) {
                // source guarenteed to exists, just check target
                if (!spanningTree.exists(inputTuple.getTarget(), targetState)) {
                    processTransition(spanningTree, inputTuple.getSource(), sourceState, inputTuple.getTarget(), targetState, 0);
                }
            }
        }


        // metric recording
        Long elapsedTime = System.nanoTime() - startTime;
        //populate histograms
        fullHistogram.update(elapsedTime);
        timer.stop();
        if(!transitions.isEmpty()) {
            // it implies that edge is processed
            processedHistogram.update(elapsedTime);
        }
    }

}
