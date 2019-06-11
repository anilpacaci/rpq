package ca.uwaterloo.cs.streamingrpq.waveguide;

import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.input.*;
import ca.uwaterloo.cs.streamingrpq.util.PathSemantics;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class WaveGuideQueryRunner {

    private static Logger logger = LoggerFactory.getLogger(WaveGuideQueryRunner.class);

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

        DFA<Integer> queryDFA;
        if(queryName.equals("waveguide6")) {
            queryDFA = WaveGuideQueries.query6(pathSemantics, maxSize, predicates);
        } else if(queryName.equals("waveguide5")) {
            queryDFA = WaveGuideQueries.query5(pathSemantics, maxSize, predicates);
        } else if(queryName.equals("pvldbq1")) {
            queryDFA = WikidataQueries.pvdlbq1(pathSemantics, maxSize, predicates);
        } else if(queryName.equals("pvldbq2")) {
            queryDFA = WikidataQueries.pvdlbq2(pathSemantics, maxSize, predicates);
        } else if(queryName.equals("pvldbq3")) {
            queryDFA = WikidataQueries.pvdlbq3(pathSemantics, maxSize, predicates);
        } else if(queryName.equals("wwwq2")) {
            queryDFA = WikidataQueries.wwwq2(pathSemantics, maxSize, predicates);
        } else {
            logger.error("Not a valid queryname: " + queryName);
            return;
        }

        MetricRegistry metricRegistry = new MetricRegistry();

        queryDFA.addMetricRegistry(metricRegistry);
        // create the metrics directory
        File resultDirectory = new File(recordCSVFilePath);
        resultDirectory.mkdirs();
        CsvReporter reporter = CsvReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MICROSECONDS).build(resultDirectory);
        reporter.start(10, TimeUnit.SECONDS);

        SingleThreadedRun task = new SingleThreadedRun(queryName, stream, queryDFA);
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

        Option labelOption = new Option("l", "labels", true, "list of labels in order");
        labelOption.setArgs(Option.UNLIMITED_VALUES);
        labelOption.setValueSeparator(',');
        labelOption.setRequired(true);
        options.addOption(labelOption);

        return options;
    }
}
