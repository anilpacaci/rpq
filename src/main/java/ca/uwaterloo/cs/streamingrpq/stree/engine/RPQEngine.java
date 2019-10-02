package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;
import ca.uwaterloo.cs.streamingrpq.stree.data.Graph;
import ca.uwaterloo.cs.streamingrpq.stree.data.QueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.data.SpanningTree;
import com.codahale.metrics.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public abstract class RPQEngine<L> {

    protected MetricRegistry metricRegistry;
    protected Counter expansionCounter;
    protected Histogram fullHistogram;
    protected Histogram processedHistogram;
    protected Timer fullTimer;
    protected Meter queueMeter;

    protected Delta<Integer> delta;
    protected Graph<Integer, L> graph;
    protected QueryAutomata<L> automata;

    protected Multimap<Integer, Integer> results;

    protected RPQEngine(QueryAutomata<L> query, int capacity) {
        delta = new Delta<>(capacity);
        automata = query;
        results = HashMultimap.create();
        graph = new Graph<>(capacity);
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

    public abstract void processEdge(InputTuple<Integer, Integer, L> inputTuple);

    public abstract void processTransition(SpanningTree<Integer> tree, int parentVertex, int parentState, int childVertex, int childState);
}
