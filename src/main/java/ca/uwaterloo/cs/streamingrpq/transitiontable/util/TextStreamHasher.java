package ca.uwaterloo.cs.streamingrpq.transitiontable.util;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TextStreamHasher {

    private static Logger logger = LoggerFactory.getLogger(TextStreamHasher.class);

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(getCLIOptions(), args);
        } catch (ParseException e) {
            logger.error("Command line argument can NOT be parsed", e);
            return;
        }

        String inputFile = line.getOptionValue("i");
        String outputFile = line.getOptionValue("o");

        TextStream stream = new Yago2sTSVStream();

        stream.open(inputFile);
        PrintWriter writer;
        try {
             writer =  new PrintWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            logger.error("Output file could not be created", e);
            return;
        }

        InputTuple<Integer, Integer, Integer> tuple = stream.next();
        while (tuple != null) {
            writer.println(tuple.getSource() + " " + tuple.getLabel() + " " + tuple.getTarget());
            tuple = stream.next();
        }

        writer.close();
        stream.close();
    }

    private static Options getCLIOptions() {
        Options options = new Options();

        options.addRequiredOption("i", "input", true, "location of the input file");
        options.addRequiredOption("o", "output", true, "location of the output file");

        return options;
    }
}
