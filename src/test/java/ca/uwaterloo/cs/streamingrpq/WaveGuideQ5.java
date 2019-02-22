package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.core.SubPath;
import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.dfa.DFAEdge;
import ca.uwaterloo.cs.streamingrpq.dfa.DFANode;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sInMemoryTSVStream;
import com.google.common.collect.HashMultimap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class WaveGuideQ5 {

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
            System.err.println("Parameters file does not have all required parameters");
            return;
        }

        String filename = config.getString("input.file");
        Integer queryCount = config.getInt("query.count");
        Integer inputSize = config.getInt("input.size");
        String[] queryNames = config.getStringArray("query.names");
        String[] p0 = config.getStringArray("p.label");
        String[] p1 = config.getStringArray("p1.label");
        String[] p2 = config.getStringArray("p2.label");

        Yago2sInMemoryTSVStream stream = new Yago2sInMemoryTSVStream();

        try {
            stream.open(filename, inputSize);

            for (int i = 0; i < queryCount; i++) {
                DFA<Integer> q5 = new DFA<>();
                q5.addDFAEdge(0,1, p0[i].hashCode());
                q5.addDFAEdge(1,2, p1[i].hashCode());
                q5.addDFAEdge(2,2, p1[i].hashCode());
                q5.addDFAEdge(2,3, p2[i].hashCode());
                q5.addDFAEdge(3,3, p2[i].hashCode());
                q5.setFinalState(3);

                InputTuple<Integer, Integer, Integer> input = stream.next();

                while (input != null) {
                    //retrieve DFA nodes where transition is same as edge label
                    q5.processEdge(input);
                    // incoming edge fully processed, move to next one
                    input = stream.next();
                }

                System.out.println("total number of results for query " + queryNames[i] + " : " + q5.getResultCounter());

                //reset the stream so we can reuse it for the next query
                stream.reset();
            }
            // stream is over so we can close it and close the program
            stream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
