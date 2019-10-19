package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.codahale.metrics.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;

import java.sql.Time;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public abstract class RPQEngine<L> {

    protected MetricRegistry metricRegistry;
    protected Counter resultCounter;
    protected Histogram containingTreeHistogram;
    protected Histogram fullHistogram;
    protected Histogram processedHistogram;
    protected Histogram explicitDeletionHistogram;
    protected Histogram windowManagementHistogram;
    protected Histogram edgeCountHistogram;
    protected Timer fullTimer;

    protected ProductGraph<Integer, L> productGraph;
    protected QueryAutomata<L> automata;

    protected Queue<ResultPair<Integer>> results;

    protected int edgeCount = 0;


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
        this.processedHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("processed-histogram", this.processedHistogram);
        this.explicitDeletionHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("explicit-deletion-histogram", this.explicitDeletionHistogram);
        this.containingTreeHistogram = metricRegistry.histogram("containing-tree-counter");
        this.fullTimer = metricRegistry.timer("full-timer");
        this.windowManagementHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("window-histogram", windowManagementHistogram);
        edgeCountHistogram = metricRegistry.histogram("edgecount-histogram");

        this.productGraph.addMetricRegistry(metricRegistry);
    }

    public abstract void processEdge(InputTuple<Integer, Integer, L> inputTuple);

    public abstract void shutDown();
}
