package ca.uwaterloo.cs.streamingrpq.runtime;

import ca.uwaterloo.cs.streamingrpq.input.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.QueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.engine.RPQEngine;
import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.util.Semantics;
import ca.uwaterloo.cs.streamingrpq.transitiontable.util.PathSemantics;
import ca.uwaterloo.cs.streamingrpq.transitiontable.waveguide.SingleThreadedRun;
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

        String semantics = line.getOptionValue("ps");
        Semantics pathSemantics = Semantics.fromValue(semantics);

        String recordCSVFilePath = line.getOptionValue("r");

        String[] predicateString = line.getOptionValues("l");
        Integer[] predicates = Arrays.stream(predicateString).map(s -> s.hashCode()).toArray(Integer[]::new);

        TextStream stream;

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

        stream.open(filename, inputSize, startTimestamp);



        RPQEngine rpq;
        QueryAutomata<String> query;
        SingleThreadedRun task;
        try {
            query = MazeQueries.getMazeQuery(queryName, predicateString);
        } catch (IllegalArgumentException e) {
            logger.error("Error creating the query", e);
            return;
        }


        if(semantics.equals(Semantics.ARBITRARY)) {
            rpq = new WindowedRAPQ<String>(query, maxSize, windowSize, slideSize, threadCount);
        } else {
            rpq = new WindowedRSPQ<String>(query, maxSize, windowSize, slideSize, threadCount);
        }

        task = new SingleThreadedRun<String>(queryName, stream, rpq);

        MetricRegistry metricRegistry = new MetricRegistry();

        rpq.addMetricRegistry(metricRegistry);
        // create the metrics directory
        File resultDirectory = new File(recordCSVFilePath);
        resultDirectory.mkdirs();
        CsvReporter reporter = CsvReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MICROSECONDS).build(resultDirectory);
        reporter.start(1, TimeUnit.SECONDS);

        try {
            task.call();
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

        Option labelOption = new Option("l", "labels", true, "list of labels in order");
        labelOption.setArgs(Option.UNLIMITED_VALUES);
        labelOption.setValueSeparator(',');
        labelOption.setRequired(true);
        options.addOption(labelOption);

        return options;
    }
}
