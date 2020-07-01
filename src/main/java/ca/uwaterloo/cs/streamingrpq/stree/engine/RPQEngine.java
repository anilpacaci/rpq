package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNodeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.SpanningTreeRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.TreeNodeRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;
import ca.uwaterloo.cs.streamingrpq.stree.util.Semantics;
import com.codahale.metrics.*;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import java.util.Queue;
import java.util.Set;
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
    protected Histogram fullProcessedHistogram;
    protected Histogram windowManagementHistogram;
    protected Histogram edgeCountHistogram;
    protected Timer fullTimer;

    protected ProductGraph<Integer, L> productGraph;
    protected Automata<L> automata;

    protected Set<ResultPair<Integer>> results;

    protected int edgeCount = 0;


    protected RPQEngine(Automata<L> query, int capacity) {
        automata = query;
        results = Sets.newHashSet();
        productGraph = new ProductGraph<>(capacity, query);
    }

    public Set<ResultPair<Integer>> getResults() {
        return  results;
    }

    public long getResultCount() {
        return resultCounter.getCount();
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        // register all the matrics
        this.metricRegistry = metricRegistry;

        // a counter that keeps track of total result count
        this.resultCounter = metricRegistry.counter("result-counter");

        // histogram that keeps track of processing append only  tuples in teh stream if there is a corresponding edge in the product graph
        this.processedHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("processed-histogram", this.processedHistogram);

        // histogram that keeps track of processing of explicit negative tuples
        this.explicitDeletionHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("explicit-deletion-histogram", this.explicitDeletionHistogram);

        // histogram responsible of tracking how many trees are affected by each input stream edge
        this.containingTreeHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("containing-tree-counter", this.containingTreeHistogram);

        // measures the time spent on processing each edge from the input stream
        this.fullTimer = new Timer(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("full-timer", this.fullTimer);

        // histogram responsible to measure time spent in Window Expiry procedure at every slide interval
        this.windowManagementHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("window-histogram", windowManagementHistogram);

        // histogram responsible of keeping track number of edges in each side of a window
        edgeCountHistogram = metricRegistry.histogram("edgecount-histogram");

        this.productGraph.addMetricRegistry(metricRegistry);
    }

    public abstract void processEdge(InputTuple<Integer, Integer, L> inputTuple);

    public abstract void shutDown();

    /**
     * Create a windowed RPQ engine ready to execute queries based on given parameters
     * @param query Automata representation of the standing RPQ
     * @param capacity Number of spanning trees and index size
     * @param windowSize Window size in terms of milliseconds
     * @param slideSize Slide size in terms of milliseconds
     * @param numOfThreads Total number of threads for ExpansionExecutor Pool
     * @param semantics arbitrary or simple
     * @param <L> Type of tuple labels and automata transitions
     * @return
     */
    public static <L> RPQEngine<L> createWindowedRPQEngine(Automata<L> query, int capacity, long windowSize, long slideSize, int numOfThreads, Semantics semantics) {
        RPQEngine<L> windowedEngine;

        if(semantics.equals(Semantics.ARBITRARY)) {
            windowedEngine = new WindowedRPQ<L, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>(query, capacity, windowSize, slideSize, numOfThreads, semantics);
        } else {
            windowedEngine = new WindowedRPQ<L, SpanningTreeRSPQ<Integer>, TreeNodeRSPQ<Integer>>(query, capacity, windowSize, slideSize, numOfThreads, semantics);
        }

        return windowedEngine;
    }

    /**
     * Create a windowed RPQ engine for single source RPQ evaluation under arbitrary path semantics
     * @param query Automata representation of the standing RPQ
     * @param capacity Number of spanning trees and index size
     * @param windowSize Window size in terms of milliseconds
     * @param slideSize Slide size in terms of milliseconds
     * @param allPairs controls whether a single source for quey can be specified
     * @param sourceVertex the root node for evaluation, only used if <code>allPairs</code> is set to <code>true</code>
     * @param <L> Type of tuple labels and automata transitions
     * @return
     */
    public static <L> RPQEngine<L> createWindowedRPQEngine(Automata<L> query, int capacity, long windowSize, long slideSize, boolean allPairs, int sourceVertex) {
        RPQEngine<L>  windowedEngine = new WindowedRPQ<L, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>(query, capacity, windowSize, slideSize, allPairs, sourceVertex);
        return windowedEngine;
    }
}
