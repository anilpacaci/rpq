package ca.uwaterloo.cs.streamingrpq.runtime;

import ca.uwaterloo.cs.streamingrpq.input.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.QueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNodeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.engine.RPQEngine;
import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRPQ;
import ca.uwaterloo.cs.streamingrpq.stree.util.Semantics;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class STQueryRunner {

    private static Logger logger = LoggerFactory.getLogger(STQueryRunner.class);

    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(getCLIOptions(), args);
        } catch (ParseException e) {
            logger.error("Command line argument can NOT be parsed", e);
            return;
        }
        String filename = line.getOptionValue("f");
        String inputType = line.getOptionValue("t");
        Integer inputSize = Integer.parseInt(line.getOptionValue("s"));
        Integer maxSize = Integer.parseInt(line.getOptionValue("s"));
        String queryName = line.getOptionValue("n");
        Long windowSize = Long.parseLong(line.getOptionValue("ws"));
        Long slideSize = Long.parseLong(line.getOptionValue("ss"));
        Long startTimestamp = Long.parseLong(line.getOptionValue("st", "0"));
        Integer threadCount = Integer.parseInt(line.getOptionValue("tc", "1"));
        Integer deletionPercentage = Integer.parseInt(line.getOptionValue("dr", "0"));

        String semantics = line.getOptionValue("ps");
        Semantics pathSemantics = Semantics.fromValue(semantics);

        String recordCSVFilePath = line.getOptionValue("r");

        String[] predicateString = line.getOptionValues("l");
        Integer[] predicates = Arrays.stream(predicateString).map(s -> s.hashCode()).toArray(Integer[]::new);

        TextFileStream stream;

        switch (inputType) {
            case "tsv":
                stream = new Yago2sTSVStream();
                break;
            case "hash":
                stream = new Yago2sHashStream();
                break;
            case "text":
                stream = new SimpleTextStream();
                break;
            case "snap-sx":
                stream = new StackOverflowStream();
                break;
            case "ldbc":
                stream = new LDBCStream();
                break;
            default:
                stream = new Yago2sTSVStream();
        }

        RPQEngine rpq;
        QueryAutomata<String> query;
        try {
            query = MazeQueries.getMazeQuery(queryName, predicateString);
        } catch (IllegalArgumentException e) {
            logger.error("Error creating the query", e);
            return;
        }


        rpq = new WindowedRPQ<String, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>(query, maxSize, windowSize, slideSize, threadCount, pathSemantics);

        stream.open(filename, inputSize, startTimestamp, deletionPercentage);

        MetricRegistry metricRegistry = new MetricRegistry();

        rpq.addMetricRegistry(metricRegistry);
        // create the metrics directory
        File resultDirectory = new File(recordCSVFilePath);
        resultDirectory.mkdirs();
        CsvReporter reporter = CsvReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MICROSECONDS).build(resultDirectory);
        reporter.start(1, TimeUnit.SECONDS);

        try {
            InputTuple<Integer, Integer, String> input = stream.next();
            logger.info("Query " + queryName + " is starting!");

            while (input != null) {
                //retrieve DFA nodes where transition is same as edge label
                rpq.processEdge(input);
                // incoming edge fully processed, move to next one
                input = stream.next();

                if(Thread.currentThread().isInterrupted()) {
                    logger.info("Query " + queryName + " is interrupted");
                    break;
                }
            }
            logger.info("total number of results for query " + queryName + " : " + rpq.getResultCount());
        } catch (Exception e) {
            logger.error("Experiment on main-thread encountered an error: ", e);
        }

        // shutdown the reporter
        reporter.stop();
        reporter.close();

        //shutdown the engine
        rpq.shutDown();

        //shut down the executor
        //reset the stream so we can reuse it for the next query
        stream.reset();
        // stream is over so we can close it and close the program
        stream.close();

    }

    private static Options getCLIOptions() {
        Options options = new Options();

        options.addRequiredOption("f", "file", true, "text file to read");
        options.addRequiredOption("t", "type", true, "input type");
        options.addRequiredOption("s", "size", true, "maximum DFST size to be allowed");
        options.addRequiredOption("n", "name", true, "name of the query to be run");
        options.addRequiredOption("ps", "semantics", true, "path semantics");
        options.addRequiredOption("r", "report-path", true, "CSV file to record execution metrics");
        options.addRequiredOption("ws", "window-size", true, "Size of the window in milliseconds");
        options.addRequiredOption("ss", "slide-size", true, "Slide of the window in milliseconds");
        options.addOption("st", "start-timestamp", true, "Starting timestamp, 0 by default");
        options.addOption("tc", "threadCount", true, "# of Threads for inter-query parallelism");
        options.addOption("dr", "deleteRatio", true, "percentage of deletions in the stream");

        Option labelOption = new Option("l", "labels", true, "list of labels in order");
        labelOption.setArgs(Option.UNLIMITED_VALUES);
        labelOption.setValueSeparator(',');
        labelOption.setRequired(true);
        options.addOption(labelOption);

        return options;
    }
}
