package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import com.codahale.metrics.Timer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public class WindowedRAPQ<L> extends RPQEngine<L> {

    private int windowSize;
    private int slideSize;

    public WindowedRAPQ(QueryAutomata<L> query, int capacity, int windowSize, int slideSize) {
        super(query, capacity);
        this.windowSize = windowSize;
        this.slideSize = slideSize;
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
            graph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
        }

        //create a spanning tree for the source node in case it does not exists
        if(transitions.keySet().contains(0) && !delta.exists(inputTuple.getSource())) {
            // if there exists a start transition with given label, there should be a spanning tree rooted at source vertex
            delta.addTree(inputTuple.getSource(), inputTuple.getTimestamp());
        }

        List<Map.Entry<Integer, Integer>> transitionList = transitions.entrySet().stream().collect(Collectors.toList());

        // for each transition that given label satisy
        for(Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();

            // iterate over spanning trees that include the source node
            for(SpanningTree spanningTree : delta.getTrees(inputTuple.getSource(), sourceState)) {
                // source is guarenteed to exists due to above loop,
                // we do not check target here as even if it exist, we might update its timetsap
                processTransition(spanningTree, inputTuple.getSource(), sourceState, inputTuple.getTarget(), targetState, inputTuple.getTimestamp());
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

    @Override
    public void processTransition(SpanningTree<Integer> tree, int parentVertex, int parentState, int childVertex, int childState, long edgeTimestamp) {
        TreeNode parentNode = tree.getNode(parentVertex, parentState);

        // either update timestamp, or create the node
        if(tree.exists(childVertex, childState)) {
            // if the child node already exists, we might need to update timestamp
            TreeNode childNode = tree.getNode(childVertex, childState);
            if(childNode.getTimestamp() < Long.min(parentNode.getTimestamp(), edgeTimestamp)) {
                // only update its timestamp if there is a younger  path
                childNode.setTimestamp(Long.min(parentNode.getTimestamp(), edgeTimestamp));
                // properly update the parent pointer
                childNode.setParent(parentNode);
            }
        } else {
            // extend the spanning tree with incoming node
            tree.addNode(parentNode, childVertex, childState, Long.min(parentNode.getTimestamp(), edgeTimestamp));

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

    /**
     * updates Delta and Spanning Trees and removes any node that is lower than the window endpoint
     * might need to traverse the entire spanning tree to make sure that there does not exists an alternative path
     */
    private void expiry(long minTimestamp) {

    }
}
