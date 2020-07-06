package ca.uwaterloo.cs.streamingrpq.ddlog.virtuoso;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.ResultPair;
import ca.uwaterloo.cs.streamingrpq.stree.engine.RPQEngine;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.google.common.collect.Sets;
import ddlog.tc.*;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.base.Sys;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import virtuoso.jena.driver.VirtDataset;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Simulation of persistent query evaluation on sliding windows over streaming graphs
 * At any given time, it stores the entire window content in a sorted array by the timestamp and in Virtuoso
 * With each slide, it issues a SPARUL query to update the Virtuoso content, then re-executes the query to update result set
 * This persistent query evaluation simulation is used in the SIGMOD'20 Streaming RPQ submission revision
 */
public class DDlogWindowedRPQ extends RPQEngine<String> {

    private static String DEFAULT_GRAPH_NAME = "window";

    private final Logger logger = LoggerFactory.getLogger(DDlogWindowedRPQ.class);

    private Deque<InputTuple<Integer, Integer, String>> windowContent;

    private long windowSize;
    private long slideSize;
    private long lastExpiry;

    private String queryPredicate;

    private Set<ResultPair<Long>> results;

    private DDlogAPI api;

    public DDlogWindowedRPQ(String label, int capacity, int numberOfThreads, long windowSize, long slideSize)  {
        super(null, capacity);
        this.windowSize = windowSize;
        this.slideSize = slideSize;
        this.lastExpiry = 0;

        // extract query predicates from the query string
        queryPredicate = label;

        results = new HashSet<>();

        logger.info("Initializing DDlog engine for label {} with {} threads", label, numberOfThreads );

        //initialize buffer
        windowContent = new ArrayDeque<>(((int) windowSize) * 10);

        // create an instance of DDlog program with one worker thread
        try {
            this.api = new DDlogAPI(numberOfThreads, null, false);
        } catch (DDlogException e) {
            logger.error("DDlog engine could not be initialized, program will be terminated", e);
            System.exit(-1);
        }
    }

    public void processEdge(InputTuple<Integer, Integer, String> inputTuple) {
        long currentTimestamp = inputTuple.getTimestamp();
        String edgePredicate = inputTuple.getLabel();

        if(!this.queryPredicate.equals(edgePredicate)) {
            // do not process the edge if it is not contained in the query
            return;
        }

        // check whether it is the slide time
        if(currentTimestamp - slideSize >= lastExpiry && currentTimestamp >= windowSize) {
            // perform window update
            expiry(currentTimestamp - windowSize);

            // update the last expiry time
            lastExpiry = currentTimestamp;
        }

        // start DDlog transaction
        tcUpdateBuilder updatebuilder = new tcUpdateBuilder();
        updatebuilder.insert_Edge(inputTuple.getSource(), inputTuple.getTarget());

        try {
            this.api.transactionStart();
            long startTime = System.nanoTime();
            updatebuilder.applyUpdates(this.api);
            tcUpdateParser.transactionCommitDumpChanges(this.api, r -> ddlogCommitCallback(r));
            long executeTime = System.nanoTime() - startTime;

            // update the full edge process histogram
            processedHistogram.update(executeTime);

            // generate the triple and update the buffer content
            windowContent.offer(inputTuple);
        } catch (DDlogException e) {
            logger.error("Error inserting tuple {}", inputTuple, e);
        }
    }

    private void expiry(long minTimestamp) {
        // first create the delete list
        List<InputTuple<Integer, Integer, String>> deleteList = windowContent.stream().filter(vt -> vt.getTimestamp() < minTimestamp).collect(Collectors.toList());

        tcUpdateBuilder updateBuilder = new tcUpdateBuilder();
        deleteList.stream().forEach(tuple -> updateBuilder.delete_Edge(tuple.getSource(), tuple.getTarget()));

        // execute the update query on DDlog
        try {
            this.api.transactionStart();
            long updateStartTime = System.nanoTime();
            updateBuilder.applyUpdates(this.api);
            tcUpdateParser.transactionCommitDumpChanges(this.api, r -> ddlogCommitCallback(r));
            long updateTime = System.nanoTime() - updateStartTime;
            logger.info("Window: " + this.lastExpiry+ "\tUpdate time " + updateTime );
            logger.info("# of results {}", results.size());

            //update histogram
            windowManagementHistogram.update(updateTime);
        } catch (DDlogException e) {
            logger.error("Error during expiry at {}", this.lastExpiry);
        }

        // finally insert new tuples to the graph, remove from the window array
        windowContent.removeIf(vt -> vt.getTimestamp() < minTimestamp);
    }

    private void ddlogCommitCallback(DDlogCommand<Object> r) {

        int relid = r.relid();
        switch (relid) {
            case tcRelation.Path:
                PathReader path = (PathReader)r.value();
                r.kind();
                if(r.kind().equals(DDlogCommand.Kind.Insert)) {
                    results.add(new ResultPair<Long>(path.s1(), path.s2()));
                } else {
                    results.remove(new ResultPair<Long>(path.s1(), path.s2()));
                }
                break;
            default: throw new IllegalArgumentException("Unknown relation id " + relid);
        }
    }

    public long getResultCount() {
        return this.resultCounter.getCount();
    }

    public void shutDown() {
        //remove the graph instance for the next execution
    }

}
