package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import com.codahale.metrics.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IncrementalRAPQ<L> {

    // metric tools
    private MetricRegistry metricRegistry;
    private Counter expansionCounter;
    private Histogram fullHistogram;
    private Histogram processedHistogram;
    private Timer fullTimer;
    private Meter queueMeter;

    private Delta<Integer> delta;
    private Graph<Integer, L> graph;
    private QueryAutomata<L> automata;

    private Multimap<Integer, Integer> results;

    public IncrementalRAPQ(QueryAutomata<L> query, int capacity) {
        delta = new Delta<>(capacity);
        automata = query;
        results = HashMultimap.create();
        graph = new Graph<>(capacity);
    }

    public void processTransition(SpanningTree<Integer> tree,  int parentVertex, int parentState, int childVertex, int childState) {
        TreeNode parentNode = tree.getNode(parentVertex, parentState);
        // extend the spanning tree with incoming noe
        tree.addNode(parentNode, childVertex, childState, parentNode.getTimestamp());

        // add this pair to results if it is a final state
        if(automata.isFinalState(childState)) {
            results.put(tree.getRootVertex(), childVertex);
        }

        // get all the forward edges of the new extended node
        Multimap<L, Integer> forwardEdges = graph.getForwardEdges(childVertex);

        if(forwardEdges == null) {
            // TODO better nul handling
            // end recursion if node has no forward edges
            return;
        }

        for(Map.Entry<L, Integer> forwardEdge : forwardEdges.entries()) {
            Integer targetState = automata.getTransition(childState, forwardEdge.getKey());
            if(targetState != null && !tree.exists(forwardEdge.getValue(), targetState)) {
                // recursive call as the target of the forwardEdge has not been visited in state targetState before
                processTransition(tree, childVertex, childState, forwardEdge.getValue(), targetState);
            }
        }
    }

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
            graph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel());
        }

        //create a spanning tree for the source node in case it does not exists
        if(transitions.keySet().contains(0) && !delta.exists(inputTuple.getSource())) {
            // if there exists a start transition with given label, there should be a spanning tree rooted at source vertex
            delta.addTree(inputTuple.getSource());
        }

        List<Map.Entry<Integer, Integer>> transitionList = transitions.entrySet().stream().collect(Collectors.toList());

        // for each transition that given label satisy
        for(Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();

            // iterate over spanning trees that include the source node
            for(SpanningTree spanningTree : delta.getTrees(inputTuple.getSource(), sourceState)) {
                // if the source already exists, but not the target
                if (spanningTree.exists(inputTuple.getSource(), sourceState) && !spanningTree.exists(inputTuple.getTarget(), targetState)) {
                    processTransition(spanningTree, inputTuple.getSource(), sourceState, inputTuple.getTarget(), targetState);
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

    public Multimap<Integer, Integer> getResults() {
        return  results;
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        // register all the matrics
        this.expansionCounter = metricRegistry.counter("expansion-counter");
        this.fullHistogram = metricRegistry.histogram("full-histogram");
        this.processedHistogram = metricRegistry.histogram("processed-histogram");
        this.fullTimer = metricRegistry.timer("full-timer");
        this.queueMeter = metricRegistry.meter("queue-meter");
    }
}
