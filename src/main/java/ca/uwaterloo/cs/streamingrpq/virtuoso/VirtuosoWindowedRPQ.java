package ca.uwaterloo.cs.streamingrpq.virtuoso;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRPQ;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import org.apache.jena.base.Sys;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import virtuoso.jena.driver.VirtDataset;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtModel;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Simulation of persistent query evaluation on sliding windows over streaming graphs
 * At any given time, it stores the entire window content in a sorted array by the timestamp and in Virtuoso
 * With each slide, it issues a SPARUL query to update the Virtuoso content, then re-executes the query to update result set
 * This persistent query evaluation simulation is used in the SIGMOD'20 Streaming RPQ submission revision
 */
public class VirtuosoWindowedRPQ {

    private static String DEFAULT_GRAPH_NAME = "window";

    private final Logger logger = LoggerFactory.getLogger(VirtuosoWindowedRPQ.class);

    private MetricRegistry metricRegistry;
    private Counter resultCounter;
    private Histogram windowManagementHistogram;
    private Histogram queryExecutionHistogram;
    private Histogram resultParsingHistogram;

    private Deque<VirtuosoTriple> windowContent;
    private Deque<VirtuosoTriple> insertBuffer;

    private long windowSize;
    private long slideSize;
    private long lastExpiry;


    private Model virtuosoModel;

    private String queryString;

    public VirtuosoWindowedRPQ(String url, String username, String password, String query, long windowSize, long slideSize) {
        this.windowSize = windowSize;
        this.slideSize = slideSize;
        this.queryString = query;
        this.lastExpiry = 0;

        logger.info("Opening virtuoso connection " + url );
        VirtDataset virtGraph = new VirtDataset(url, username, password);
        if(!virtGraph.containsNamedModel(DEFAULT_GRAPH_NAME)) {
            logger.info("Named mode does not exist:" + DEFAULT_GRAPH_NAME);
        }
        // create Virtuoso graph connection
        virtuosoModel = virtGraph.getNamedModel(DEFAULT_GRAPH_NAME);

        //initialize buffer
        windowContent = new ArrayDeque<>(((int) windowSize) * 10);
        insertBuffer = new ArrayDeque<>(((int) slideSize) * 10);

    }

    public void processEdge(InputTuple<Integer, Integer, String> inputTuple) {
        long currentTimestamp = inputTuple.getTimestamp();

        // generate the triple and update the buffer content
        VirtuosoTriple vt = new VirtuosoTriple(inputTuple, virtuosoModel);
        insertBuffer.offer(vt);

        // check whether it is the slide time
        if(currentTimestamp - slideSize >= lastExpiry && currentTimestamp >= windowSize) {
            // perform window update
            updateWindow(currentTimestamp - windowSize);

            // perform the query
            executeQuery();

            // update the last expiry time
            lastExpiry = currentTimestamp;
        }

    }

    private void updateWindow(long minTimestamp) {
        // first create the insert list
        List<Statement> insertList = insertBuffer.stream().map(vt -> vt.getStatement()).collect(Collectors.toList());
        List<Statement> deleteList = windowContent.stream().filter(vt -> vt.getTimestamp() < minTimestamp).map(vt -> vt.getStatement()).collect(Collectors.toList());

        // execute the update query on Virtuoso
        long insertStartTime = System.nanoTime();
        virtuosoModel.add(insertList);

        virtuosoModel.remove(deleteList);
        long updateTime = System.nanoTime() - insertStartTime;

        // finally insert new tuples to the graph
        windowContent.addAll(insertBuffer);

        //clear buffer
        insertBuffer.clear();

        logger.info("Window: " + this.lastExpiry+ "\tUpdate time " + updateTime );


        //update histogram
        windowManagementHistogram.update(updateTime);
    }

    private void executeQuery() {
        QueryExecution vqe = VirtuosoQueryExecutionFactory.create(queryString, virtuosoModel);


        long queryStartTime = System.nanoTime();
        ResultSet resultSet = vqe.execSelect();
        long queryExecutionTime = System.nanoTime() - queryStartTime;

        long parseStartTime = System.nanoTime();
        while (resultSet.hasNext()) {
            QuerySolution solution = resultSet.nextSolution();
            resultCounter.inc();
        }
        long queryParseTime = System.nanoTime() - parseStartTime;

        logger.info("Window: " + this.lastExpiry+ "\tQuery execution time " + queryExecutionTime );

        //update histograms
        this.queryExecutionHistogram.update(queryExecutionTime);
        this.resultParsingHistogram.update(queryParseTime);

    }

    public long getResultCount() {
        return this.resultCounter.getCount();
    }

    public void shutDown() {
        //remove the graph instance for the next execution
        virtuosoModel.removeAll();

    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        // register all the matrics
        this.metricRegistry = metricRegistry;

        // a counter that keeps track of total result count
        this.resultCounter = metricRegistry.counter("result-counter");

        // histogram responsible to measure time spent in Window Expiry procedure at every slide interval
        this.windowManagementHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("window-histogram", windowManagementHistogram);

        //histogram responsible to measure the time spent in query execution
        this.queryExecutionHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("query-execution", queryExecutionHistogram);

        //histogram responsible to measure the time spent in query parsing
        this.resultParsingHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("result-parsing", resultParsingHistogram);
    }

}
