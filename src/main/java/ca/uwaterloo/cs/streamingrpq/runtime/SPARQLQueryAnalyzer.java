package ca.uwaterloo.cs.streamingrpq.runtime;

import ca.uwaterloo.cs.streamingrpq.stree.query.BricsAutomata;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

public class SPARQLQueryAnalyzer {

    private static Logger logger = LoggerFactory.getLogger(SPARQLQueryAnalyzer.class);

    private static String OUTPUT_FILENAME = "query-info.csv";
    private static String SPARQL_FILE_EXTENSION = "sparql";
    private static String QUERY_VISUAL_DIRECTORY = "graphs";

    private static String[] CSV_HEADER = {
            "query-name", "query-num", "query-length", "kleene-star", "alternation", "predicates", "distinct-predicates",
            "nfa-states", "nfa-transitions", "dfa-states", "dfa-transitions", "min-dfa-states", "min-dfa-transitions"
    };

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(getCLIOptions(), args);
        } catch (ParseException e) {
            logger.error("Command line argument can NOT be parsed", e);
            return;
        }

        String queryFolderFileName = line.getOptionValue("q");
        String recordCSVFilePath = line.getOptionValue("r");

        Path outputCSVFilePath = Paths.get(recordCSVFilePath, OUTPUT_FILENAME);

        // generate the directory for query visuals
        File queryGraphDirectory = FileUtils.getFile(recordCSVFilePath, QUERY_VISUAL_DIRECTORY);
        queryGraphDirectory.mkdirs();

        //generate the aggregate CSV file to be written
        CSVPrinter csvPrinter;
        try {
            BufferedWriter writer = Files.newBufferedWriter(outputCSVFilePath);
            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(CSV_HEADER));
        } catch (IOException e) {
            logger.error("Result CSV file cannot be created: " + outputCSVFilePath.getFileName(), e );
            return;
        }

        // list all sparql queries in the folder
        Collection<File> queryFiles = FileUtils.listFiles(FileUtils.getFile(queryFolderFileName), new String[]{SPARQL_FILE_EXTENSION}, false);

        for(File queryFile : queryFiles) {
            String queryName = queryFile.getName();
            int queryNum = Integer.parseInt(queryName.substring(queryName.indexOf("-") + 1, queryName.indexOf(SPARQL_FILE_EXTENSION) - 1));

            BricsAutomata automata = gMarkQueries.getQuery(queryFolderFileName, queryName);

            int queryLength = automata.getAlternationCount() + automata.getKleeneStarCount() + automata.getPredicateCount();
            int distinctLabels = automata.getAlphabet().size();

            // generate the csv entry for the query
            try {
                csvPrinter.printRecord(
                        queryName, queryNum, queryLength, automata.getKleeneStarCount(), automata.getAlternationCount(),
                        automata.getPredicateCount(), distinctLabels, automata.getNfaStates(), automata.getNfaTransitions(),
                        automata.getDfaStates(), automata.getDfaTransitions(), automata.getMinimalDFAStates(), automata.getMinimalDFATransitions()
                );
            } catch (IOException e) {
                logger.error("Entry on " + queryName + " could not be added to CSV file: " + outputCSVFilePath.getFileName(), e );
            }

            //generate the graphviz visualization for the query
            File imageFile = FileUtils.getFile(queryGraphDirectory, queryNum + ".png");
            automata.generateTransitiongraph(imageFile.getAbsolutePath());
        }

        // finally close the csvPrinter
        try {
            csvPrinter.close(true);
        } catch (IOException e) {
            logger.error("Could not close the CSV printer " + outputCSVFilePath.toString(), e);
        }
    }

    private static Options getCLIOptions() {

        Options options = new Options();

        options.addRequiredOption("q", "query-directory", true, "Directory containing SPARQL queries ");
        options.addRequiredOption("r", "report-directory", true, "Directory to store CSV files that record execution metrics");

        return options;
    }
}
