package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public class WindowedRAPQ<L> extends RPQEngine<L> {

    private long windowSize;
    private long slideSize;
    private long lastExpiry = 0;

    private Queue<TreeExpansionJob> jobQueue;

    private ExecutorService executorService;


    private final Logger LOG = LoggerFactory.getLogger(WindowedRAPQ.class);

    protected Histogram windowManagementHistogram;

    public WindowedRAPQ(QueryAutomata<L> query, int capacity, long windowSize, long slideSize) {
        this(query, capacity, windowSize, slideSize, 1);
    }

    public WindowedRAPQ(QueryAutomata<L> query, int capacity, long windowSize, long slideSize, int numOfThreads) {
        super(query, capacity);
        this.windowSize = windowSize;
        this.slideSize = slideSize;
        this.executorService = Executors.newFixedThreadPool(numOfThreads);
        this.jobQueue = new ArrayDeque<>(Constants.EXPECTED_TREES);
    }

    @Override
    public void addMetricRegistry(MetricRegistry metricRegistry) {
        windowManagementHistogram = metricRegistry.histogram("window-histogram");
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
            LOG.info("Expiry procedure at timestamp: {}", currentTimestamp);
            // its slide time, maintain the window
            Long windowStartTime = System.nanoTime();
            expiry(currentTimestamp - windowSize);
            lastExpiry = currentTimestamp;
            Long windowElapsedTime = System.nanoTime() - windowStartTime;
            windowManagementHistogram.update(windowElapsedTime);
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
            productGraph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
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

            Collection<SpanningTree> containingTrees = delta.getTrees(inputTuple.getSource(), sourceState);
            treeCount += containingTrees.size();
            // iterate over spanning trees that include the source node
            for(SpanningTree<Integer> spanningTree : containingTrees) {
                // source is guarenteed to exists due to above loop,
                // we do not check target here as even if it exist, we might update its timetsap
                TreeNode<Integer> parentNode = spanningTree.getNode(inputTuple.getSource(), sourceState);
                //processTransition(spanningTree, parentNode, inputTuple.getTarget(), targetState, inputTuple.getTimestamp());
                jobQueue.offer(new TreeExpansionJob(spanningTree, parentNode, inputTuple.getTarget(), targetState, inputTuple.getTimestamp()));
            }
        }

        while(!jobQueue.isEmpty()) {
            TreeExpansionJob job = jobQueue.remove();
            processTransition(job.getSpanningTree(), job.getParentNode(), job.getTargetVertex(), job.getTargetState(), job.getEdgeTimestamp());
        }

        // metric recording
        Long edgeElapsedTime = System.nanoTime() - edgeStartTime;
        //populate histograms
        fullHistogram.update(edgeElapsedTime);
        timer.stop();
        // if the incoming edge is not discarded
        if(!transitions.isEmpty()) {
            // it implies that edge is processed
            processedHistogram.update(edgeElapsedTime);
            containingTreeHistogram.update(treeCount);
        }

    }

    @Override
    public void processTransition(SpanningTree<Integer> tree, TreeNode<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
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
            TreeNode<Integer> childNode;
            if(parentNode.equals(tree.getRootNode())) {
                childNode = tree.addNode(parentNode, childVertex, childState, edgeTimestamp);
                parentNode.setTimestamp(edgeTimestamp);
            }
            else {
                childNode = tree.addNode(parentNode, childVertex, childState, Long.min(parentNode.getTimestamp(), edgeTimestamp));
            }
            // add this pair to results if it is a final state
            if (automata.isFinalState(childState)) {
                results.put(tree.getRootVertex(), childVertex);
                resultCounter.inc();
            }

            // get all the forward edges of the new extended node
            Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);

            if (forwardEdges == null) {
                // TODO better nul handling
                // end recursion if node has no forward edges
                return;
            } else {
                // there are forward edges, iterate over them
                for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                    // recursive call as the target of the forwardEdge has not been visited in state targetState before
                    //processTransition(tree, childNode, forwardEdge.getTarget(), targetState, forwardEdge.getTimestamp());
                    jobQueue.offer(new TreeExpansionJob(tree, childNode, forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState(), forwardEdge.getTimestamp()));
                }
            }
        }
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
        // first remove the expired edges from the productGraph
        productGraph.removeOldEdges(minTimestamp);
        // then maintain the spanning trees, not that spanning trees are maintained without knowing which edge is deleted
        //delta.expiry(minTimestamp, productGraph, automata);
        delta.batchExpiry(minTimestamp, productGraph, automata);
    }
}
