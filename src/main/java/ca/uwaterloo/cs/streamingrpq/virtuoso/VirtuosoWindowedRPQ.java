package ca.uwaterloo.cs.streamingrpq.virtuoso;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import virtuoso.jena.driver.*;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
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
    private Histogram insertTripleHistogram;
    private Histogram queryExecutionHistogram;
    private Histogram resultParsingHistogram;

    private Deque<VirtuosoTriple> windowContent;

    private long windowSize;
    private long slideSize;
    private long lastExpiry;

    private Set<String> queryPredicates;

    private Model virtuosoModel;

    private QueryExecution vqe;

    public VirtuosoWindowedRPQ(String url, String username, String password, String query, long windowSize, long slideSize) {
        this.windowSize = windowSize;
        this.slideSize = slideSize;
        this.lastExpiry = 0;

        // extract query predicates from the query string
        queryPredicates = extractQueryPredicates(query);

        logger.info("Opening virtuoso connection " + url );
        VirtDataset virtGraph = new VirtDataset(url, username, password);
        if(virtGraph.containsNamedModel(DEFAULT_GRAPH_NAME)) {
            logger.info("Named model is removed:" + DEFAULT_GRAPH_NAME);
            virtGraph.removeNamedModel(DEFAULT_GRAPH_NAME);
        }
        // create Virtuoso graph connection
        virtuosoModel = virtGraph.getNamedModel(DEFAULT_GRAPH_NAME);

        //initialize buffer
        windowContent = new ArrayDeque<>(((int) windowSize) * 10);

        //VQE cache object ot be used for the query
        vqe = VirtuosoQueryExecutionFactory.create(query, virtuosoModel);
    }

    public void processEdge(InputTuple<Integer, Integer, String> inputTuple) {
        long currentTimestamp = inputTuple.getTimestamp();
        String edgePredicate = inputTuple.getLabel();

        if(!this.queryPredicates.contains(edgePredicate)) {
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

        // first insert the new triple
        // generate the triple and update the buffer content
        VirtuosoTriple vt = new VirtuosoTriple(inputTuple, virtuosoModel);
        insertTriple(vt);

        // finally execute the query
            executeQuery();
    }

    private void insertTriple(VirtuosoTriple vt) {
        // insert the triple into in memory window content
        windowContent.offer(vt);

        // then add it to Virtuoso graph
        Statement st = vt.getStatement();

        long insertStartTime = System.nanoTime();
        virtuosoModel.add(st);
        long insertTime = System.nanoTime() - insertStartTime;

        insertTripleHistogram.update(insertTime);
    }

    private void expiry(long minTimestamp) {
        // first create the delete list
        List<Statement> deleteList = windowContent.stream().filter(vt -> vt.getTimestamp() < minTimestamp).map(vt -> vt.getStatement()).collect(Collectors.toList());

        // execute the update query on Virtuoso
        long updateStartTime = System.nanoTime();
        virtuosoModel.remove(deleteList);
        long updateTime = System.nanoTime() - updateStartTime;

        // finally insert new tuples to the graph, remove from the window array
        windowContent.removeIf(vt -> vt.getTimestamp() < minTimestamp);

        logger.info("Window: " + this.lastExpiry+ "\tUpdate time " + updateTime );

        //update histogram
        windowManagementHistogram.update(updateTime);
    }

    private void executeQuery() {
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

        // histogram responsible to measure time spent adding new triples
        this.insertTripleHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("insert-histogram", insertTripleHistogram);

        //histogram responsible to measure the time spent in query execution
        this.queryExecutionHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("query-execution", queryExecutionHistogram);

        //histogram responsible to measure the time spent in query parsing
        this.resultParsingHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("result-parsing", resultParsingHistogram);
    }

    /**
     * Extract all query predicates from a given query String
     * @param queryString
     * @return
     */
    private static Set<String> extractQueryPredicates(String queryString) {
        String[] extracts = StringUtils.substringsBetween(queryString, "<", ">");

        Set<String> predicates = Sets.newHashSet(extracts);

        return predicates;
    }

}
