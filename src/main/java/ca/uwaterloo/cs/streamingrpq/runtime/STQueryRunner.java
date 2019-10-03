package ca.uwaterloo.cs.streamingrpq.runtime;

import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sHashStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;
import ca.uwaterloo.cs.streamingrpq.stree.data.QueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.engine.IncrementalRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.engine.RPQEngine;
import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRAPQ;
import ca.uwaterloo.cs.streamingrpq.transitiontable.util.PathSemantics;
import ca.uwaterloo.cs.streamingrpq.transitiontable.waveguide.SingleThreadedRun;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.googlecode.cqengine.query.simple.In;
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

        String semantics = line.getOptionValue("ps");
        PathSemantics pathSemantics = PathSemantics.fromValue(semantics);

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
            default:
                stream = new Yago2sTSVStream();
        }

        stream.open(filename, inputSize);


        QueryAutomata<Integer> query;

        if(queryName.equals("waveguide6")) {
            query = new QueryAutomata<>(4);
            query.addTransition(0, predicates[0], 1);
            query.addTransition(1, predicates[1], 2);
            query.addTransition(2, predicates[2], 3);
            query.addTransition(3, predicates[0], 1);
            query.addFinalState(3);

        } else if(queryName.equals("waveguide5")) {
            query = new QueryAutomata<>(4);
            query.addTransition(0, predicates[0], 1);
            query.addTransition(1, predicates[1], 2);
            query.addTransition(2, predicates[1], 2);
            query.addTransition(2, predicates[2], 3);
            query.addTransition(3, predicates[2], 3);
            query.addFinalState(3);
        } else {
            logger.error("Not a valid queryname: " + queryName);
            return;
        }

        RPQEngine<Integer> rapq = new WindowedRAPQ<Integer>(query, maxSize, windowSize, slideSize);

        MetricRegistry metricRegistry = new MetricRegistry();

        rapq.addMetricRegistry(metricRegistry);
        // create the metrics directory
        File resultDirectory = new File(recordCSVFilePath);
        resultDirectory.mkdirs();
        CsvReporter reporter = CsvReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MICROSECONDS).build(resultDirectory);
        reporter.start(10, TimeUnit.SECONDS);

        SingleThreadedRun task = new SingleThreadedRun(queryName, stream, rapq);
        try {
            task.call();
        } catch (Exception e) {
            logger.error("Experiment on main-thread encountered an error: ", e);
        }

        // shutdown the reporter
        reporter.stop();
        reporter.close();

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

        Option labelOption = new Option("l", "labels", true, "list of labels in order");
        labelOption.setArgs(Option.UNLIMITED_VALUES);
        labelOption.setValueSeparator(',');
        labelOption.setRequired(true);
        options.addOption(labelOption);

        return options;
    }
}
