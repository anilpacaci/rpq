package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import com.codahale.metrics.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;

import java.util.Queue;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public abstract class RPQEngine<L> {

    protected MetricRegistry metricRegistry;
    protected Counter resultCounter;
    protected Histogram containingTreeHistogram;
    protected Histogram fullHistogram;
    protected Histogram processedHistogram;
    protected Timer fullTimer;

    protected ProductGraph<Integer, L> productGraph;
    protected QueryAutomata<L> automata;

    protected Queue<ResultPair<Integer>> results;

    protected RPQEngine(QueryAutomata<L> query, int capacity) {
        automata = query;
        results = Queues.newConcurrentLinkedQueue();
        productGraph = new ProductGraph<>(capacity, query);
    }

    public Queue<ResultPair<Integer>> getResults() {
        return  results;
    }

    public long getResultCount() {
        return resultCounter.getCount();
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        // register all the matrics
        this.resultCounter = metricRegistry.counter("result-counter");
        this.fullHistogram = metricRegistry.histogram("full-histogram");
        this.processedHistogram = metricRegistry.histogram("processed-histogram");
        this.containingTreeHistogram = metricRegistry.histogram("containing-tree-counter");
        this.fullTimer = metricRegistry.timer("full-timer");
        this.productGraph.addMetricRegistry(metricRegistry);
    }

    public abstract void processEdge(InputTuple<Integer, Integer, L> inputTuple);

    public abstract void shutDown();
}
