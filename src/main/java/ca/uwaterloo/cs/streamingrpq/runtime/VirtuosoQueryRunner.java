package ca.uwaterloo.cs.streamingrpq.runtime;

import ca.uwaterloo.cs.streamingrpq.input.*;
import ca.uwaterloo.cs.streamingrpq.virtuoso.VirtuosoWindowedRPQ;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class VirtuosoQueryRunner {

    private static Logger logger = LoggerFactory.getLogger(VirtuosoQueryRunner.class);

    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(getCLIOptions(), args);
        } catch (ParseException e) {
            logger.error("Command line argument can NOT be parsed", e);
            return;
        }

        String url = line.getOptionValue("url", "localhost:1111");
        String user = line.getOptionValue("u", "dba");
        String password = line.getOptionValue("pw", "dba");

        // parse all necessary command line options
        String filename = line.getOptionValue("f");
        String queryFolder = line.getOptionValue("q");
        String inputType = line.getOptionValue("t");
        Integer inputSize = Integer.parseInt(line.getOptionValue("s"));
        Integer maxSize = Integer.parseInt(line.getOptionValue("s"));
        String queryName = line.getOptionValue("n");
        Long windowSize = Long.parseLong(line.getOptionValue("ws"));
        Long slideSize = Long.parseLong(line.getOptionValue("ss"));
        Long startTimestamp = Long.parseLong(line.getOptionValue("st", "0"));
        Integer threadCount = Integer.parseInt(line.getOptionValue("tc", "1"));
        Integer deletionPercentage = Integer.parseInt(line.getOptionValue("dr", "0"));

        String recordCSVFilePath = line.getOptionValue("r");

        TextFileStream<Integer, Integer, String> stream;

        switch (inputType) {
            case "tsv":
                stream = new Yago2sTSVStream();
                break;
            case "hash":
                stream = new Yago2sHashStream();
                break;
            case "text":
                stream = new SimpleTextStreamWithExplicitDeletions();
                break;
            case "snap-sx":
                stream = new StackOverflowStream();
                break;
            case "ldbc":
                stream = new LDBCStream();
                break;
            case "gmark":
                stream = new gMarkInputStream();
                break;
            default:
                stream = new Yago2sTSVStream();
        }

        String queryString;
        try {
            queryString = gMarkQueries.getQueryString(queryFolder, queryName);
        } catch (Exception e) {
            logger.error("Error duing creation of query {}", queryName, e);
            return;
        }

        VirtuosoWindowedRPQ engine = new VirtuosoWindowedRPQ(url, user, password, queryString, windowSize, slideSize);

        // initialize and prepare the input stream for consumption
        stream.open(filename, inputSize, startTimestamp, deletionPercentage);

        // metric collection initialization
        MetricRegistry metricRegistry = new MetricRegistry();
        File resultDirectory = new File(recordCSVFilePath);
        resultDirectory.mkdirs();
        CsvReporter reporter = CsvReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MICROSECONDS).build(resultDirectory);
        reporter.start(1, TimeUnit.SECONDS);

        engine.addMetricRegistry(metricRegistry);

        try {
            InputTuple<Integer, Integer, String> input = stream.next();
            logger.info("Query " + queryName + " is starting!");

            while (input != null) {
                //retrieve DFA nodes where transition is same as edge label
                engine.processEdge(input);

                // incoming edge fully processed, move to next one
                input = stream.next();

                if(Thread.currentThread().isInterrupted()) {
                    logger.info("Query " + queryName + " is interrupted");
                    break;
                }
            }
            logger.info("total number of results for query " + queryName + " : " + engine.getResultCount());
        } catch (Exception e) {
            logger.error("Experiment on main-thread encountered an error: ", e);
        }

        // ### Query execution complete, clean-up

        // shutdown the reporter
        reporter.stop();
        reporter.close();

        //shutdown the engine
        engine.shutDown();

        //shut down the executor
        //reset the stream so we can reuse it for the next query
        stream.reset();
        // stream is over so we can close it and close the program
        stream.close();
    }

    private static Options getCLIOptions() {
        Options options = new Options();

        options.addRequiredOption("f", "file", true, "text file to read");
        options.addRequiredOption("q", "query-directory", true, "Directory containing SPARQL queries ");
        options.addRequiredOption("t", "type", true, "input type");
        options.addRequiredOption("s", "size", true, "maximum DFST size to be allowed");
        options.addRequiredOption("n", "name", true, "name of the query to be run");
        options.addRequiredOption("ps", "semantics", true, "path semantics");
        options.addRequiredOption("r", "report-directory", true, "Directory to store CSV files that record execution metrics");
        options.addRequiredOption("ws", "window-size", true, "Size of the window in milliseconds");
        options.addRequiredOption("ss", "slide-size", true, "Slide of the window in milliseconds");
        options.addOption("st", "start-timestamp", true, "Starting timestamp, 0 by default");
        options.addOption("tc", "threadCount", true, "# of Threads for inter-query parallelism");
        options.addOption("dr", "deleteRatio", true, "percentage of deletions in the stream");

        return options;
    }

}
