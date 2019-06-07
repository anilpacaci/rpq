package ca.uwaterloo.cs.streamingrpq.waveguide;

import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sInMemoryTSVStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class WaveGuideQueryRunner {

    private static Logger logger = LoggerFactory.getLogger(WaveGuideQueryRunner.class);

    public static void main(String[] args) {

        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.properties().setFileName(args[0])
                                .setListDelimiterHandler(new DefaultListDelimiterHandler(','))
        );

        Configuration config = null;

        try {
            config = builder.getConfiguration();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        if( !(config.containsKey("input.file") && config.containsKey("p.label") && config.containsKey("p1.label") && config.containsKey("p2.label")) ) {
            // parameters file does not have all the required parameters
            logger.error("Parameters file does not have all required parameters");
            return;
        }

        //Single threaded executor to run the experiment
        ExecutorService executor = Executors.newSingleThreadExecutor();


        String filename = config.getString("input.file");
        Integer queryCount = config.getInt("query.count");
        Integer inputSize = config.getInt("input.size");
        Integer timeout = config.getInt("query.timeout", 10);
        String streamType = config.getString("input.stream");
        String[] queryNames = config.getStringArray("query.names");
        String[] p0 = config.getStringArray("p.label");
        String[] p1 = config.getStringArray("p1.label");
        String[] p2 = config.getStringArray("p2.label");

        TextStream stream;

        if(streamType.equals("inmemory")) {
            stream = new Yago2sInMemoryTSVStream();
        } else {
            stream = new Yago2sTSVStream();
        }

        stream.open(filename, inputSize);

        for (int i = 0; i < queryCount; i++) {

            DFA<Integer> queryDFA = WaveGuideQueries.query6(p0[i].hashCode(), p1[i].hashCode(), p2[i].hashCode());

            SingleThreadedRun task = new SingleThreadedRun(queryNames[i], stream, queryDFA);
            Future run = executor.submit(task);
            try {
                run.get(timeout, TimeUnit.MINUTES);
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
                run.cancel(true);
                logger.error("Task interrupted", e);
            }

            //reset the stream so we can reuse it for the next query
            stream.reset();
        }
        // stream is over so we can close it and close the program
        stream.close();

    }
}
