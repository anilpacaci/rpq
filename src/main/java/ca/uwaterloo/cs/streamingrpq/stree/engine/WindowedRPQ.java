package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.ObjectFactoryArbitrary;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.ObjectFactorySimple;
import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Semantics;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public class WindowedRPQ<L, T extends AbstractSpanningTree<Integer, T, N>, N extends AbstractTreeNode<Integer, T, N>> extends RPQEngine<L> {

    private long windowSize;
    private long slideSize;
    private long lastExpiry = 0;
    private Semantics semantics;

    protected Delta<Integer, T, N> delta;
    ObjectFactory<Integer, T, N> objectFactory;


    private ExecutorService executorService;

    private int numOfThreads;


    private final Logger LOG = LoggerFactory.getLogger(WindowedRPQ.class);

    /**
     * Windowed RPQ engine ready to process edges
     * @param query Automata representation of the persistent query
     * @param capacity Initial size for internal data structures. Set to approximate number of edges in a window
     * @param windowSize Size of the sliding window in milliseconds
     * @param slideSize Slide interval in milliseconds
     * @param numOfThreads Total number of executor threads
     * @param semantics Resulting path semantics: @{@link Semantics}
     */
    public WindowedRPQ(Automata<L> query, int capacity, long windowSize, long slideSize, int numOfThreads, Semantics semantics) {
        super(query, capacity);
        if (semantics.equals(Semantics.ARBITRARY)) {
            this.objectFactory = new ObjectFactoryArbitrary();
        } else {
            this.objectFactory = new ObjectFactorySimple();
        }
        this.delta =  new Delta<Integer, T, N>(capacity, objectFactory);
        this.windowSize = windowSize;
        this.slideSize = slideSize;
        this.executorService = Executors.newFixedThreadPool(numOfThreads);
        this.numOfThreads = numOfThreads;
        this.semantics = semantics;
    }

    /**
     * Windowed RPQ engine with 1 thread & Arbitrary path semantics ready to process edges
     * @param query Automata representation of the persistent query
     * @param capacity Initial size for internal data structures. Set to approximate number of edges in a window
     * @param windowSize Size of the sliding window in milliseconds
     * @param slideSize Slide interval in milliseconds

     */
    public WindowedRPQ(Automata<L> query, int capacity, long windowSize, long slideSize) {
        this(query, capacity, windowSize, slideSize, 1);
    }

    /**
     * Windowed RPQ engine with arbitrary path semantics ready to process edges
     * @param query Automata representation of the persistent query
     * @param capacity Initial size for internal data structures. Set to approximate number of edges in a window
     * @param windowSize Size of the sliding window in milliseconds
     * @param slideSize Slide interval in milliseconds
     * @param numOfThreads Total number of executor threads
     */
    public WindowedRPQ(Automata<L> query, int capacity, long windowSize, long slideSize, int numOfThreads) {
        this(query, capacity, windowSize, slideSize, 1, Semantics.ARBITRARY);
    }

    @Override
    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.delta.addMetricRegistry(metricRegistry);
        // call super function to include all other histograms
        super.addMetricRegistry(metricRegistry);
    }

    @Override
    public void processEdge(InputTuple<Integer, Integer, L> inputTuple) {
        // total number of trees expanded due this edge insertion
        int treeCount = 0;
        //for now window processing is done inside edge processing
        long currentTimestamp = inputTuple.getTimestamp();
        if(currentTimestamp - slideSize >= lastExpiry && currentTimestamp >= windowSize ) {
            // its slide time, maintain the window
            Long windowStartTime = System.nanoTime();
            expiry(currentTimestamp - windowSize);
            lastExpiry = currentTimestamp;
            Long windowElapsedTime = System.nanoTime() - windowStartTime;
            windowManagementHistogram.update(windowElapsedTime);

            //reset the edge counter
            edgeCountHistogram.update(edgeCount);
            edgeCount = 0;
        }


        // restart time for edge processing
        Long edgeStartTime = System.nanoTime();
        Timer.Context timer = fullTimer.time();
        // retrieve all transition that can be performed with this label
        Map<Integer, Integer> transitions = automata.getTransition(inputTuple.getLabel());

        if(transitions.isEmpty()) {
            // there is no transition with given label, simply return
            return;
        } else {
            // add edge to the snapshot productGraph
            if(inputTuple.isDeletion()) {
                productGraph.removeEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
                edgeCount--;
            } else {
                productGraph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
                edgeCount++;
            }
        }

            // edge is an insertion
        //create a spanning tree for the source node in case it does not exists
        if (transitions.keySet().contains(0) && !delta.exists(inputTuple.getSource())) {
            // if there exists a start transition with given label, there should be a spanning tree rooted at source vertex
            delta.addTree(inputTuple.getSource(), inputTuple.getTimestamp());
        }

        List<Future<Integer>> futureList = Lists.newArrayList();
        CompletionService<Integer> completionService = new ExecutorCompletionService<>(this.executorService);

        List<Map.Entry<Integer, Integer>> transitionList = transitions.entrySet().stream().collect(Collectors.toList());
        AbstractTreeExpansionJob treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, inputTuple.isDeletion());

        // for each transition that given label satisy
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();

            Collection<T> containingTrees = delta.getTrees(inputTuple.getSource(), sourceState);
            treeCount += containingTrees.size();

            boolean runParallel = containingTrees.size() > Constants.EXPECTED_BATCH_SIZE * this.numOfThreads;
            // iterate over spanning trees that include the source node
            for (T spanningTree : containingTrees) {
                // source is guarenteed to exists due to above loop,
                // we do not check target here as even if it exist, we might update its timetsap
                Collection<N> parentNodes = spanningTree.getNodes(inputTuple.getSource(), sourceState);
                for(N parentNode : parentNodes) {
                    //processTransition(spanningTree, parentNode, inputTuple.getTarget(), targetState, inputTuple.getTimestamp());
                    treeExpansionJob.addJob(spanningTree, parentNode, inputTuple.getTarget(), targetState, inputTuple.getTimestamp());
                    // check whether the job is full and ready to submit
                    if (treeExpansionJob.isFull()) {
                        if (runParallel) {
                            futureList.add(completionService.submit(treeExpansionJob));
                            treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, inputTuple.isDeletion());
                        } else {
                            try {
                                Integer partialResultCount = treeExpansionJob.call();
                                treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, inputTuple.isDeletion());
                                resultCounter.inc(partialResultCount);
                            } catch (Exception e) {
                                LOG.error("SpanningTreeExpansion exception on main thread", e);
                            }
                        }
                    }
                }
            }


            // wait for results of al jobs before moving to next transition to ensure that there is a single thread working on a tree
            for (int i = 0; i < futureList.size(); i++) {
                try {
                    Integer partialResultCount = completionService.take().get();
                    resultCounter.inc(partialResultCount);
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("SpanningTreeExpansion interrupted during execution", e);
                }
            }

            // if there is any remaining job in the buffer, run them in main thread
            if (!treeExpansionJob.isEmpty()) {
                try {
                    Integer partialResultCount = treeExpansionJob.call();
                    resultCounter.inc(partialResultCount);
                } catch (Exception e) {
                    LOG.error("SpanningTreeExpansion exception on main thread", e);
                }
            }

        }


        // metric recording
        Long edgeElapsedTime = System.nanoTime() - edgeStartTime;

        timer.stop();
        // if the incoming edge is not discarded
        if(!transitions.isEmpty()) {
            // it implies that edge is processed
            containingTreeHistogram.update(treeCount);

            if(inputTuple.isDeletion()) {
                // log explicit deletion time separately
                explicitDeletionHistogram.update(edgeElapsedTime);
            } else {
                // log insertion time separately
                processedHistogram.update(edgeElapsedTime);
            }
        }
    }

    private void processEdgeRAPQ() {

    }

    @Override
    public void shutDown() {
        // shutdown executors
        this.executorService.shutdown();
    }

    /**
     * updates Delta and Spanning Trees and removes any node that is lower than the window endpoint
     * might need to traverse the entire spanning tree to make sure that there does not exists an alternative path
     */
    private void expiry(long minTimestamp) {
        LOG.info("Expiry procedure at timestamp: {}", minTimestamp);
        // first remove the expired edges from the productGraph
        productGraph.removeOldEdges(minTimestamp);
        // then maintain the spanning trees, not that spanning trees are maintained without knowing which edge is deleted
        delta.expiry(minTimestamp, productGraph, this.executorService);
        //delta.batchExpiry(minTimestamp, productGraph, this.executorService);
    }

}
