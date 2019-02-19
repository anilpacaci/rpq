package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.core.Tuple;
import ca.uwaterloo.cs.streamingrpq.dfa.DFAEdge;
import ca.uwaterloo.cs.streamingrpq.dfa.DFANode;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;
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
                HashMultimap<Integer, DFAEdge<String>> dfaNodes = HashMultimap.create();

                DFANode q0 = new DFANode(0);
                DFANode q1 = new DFANode(1);
                DFANode q2 = new DFANode(2);
                DFANode q3 = new DFANode(3, true);

                q0.addUpstreamNode(q1);
                dfaNodes.put(p0[i].hashCode(), new DFAEdge(q0, q1, p0[i]));

                q1.addDownstreamNode(q0);
                q1.addUpstreamNode(q2);
                dfaNodes.put(p1[i].hashCode(), new DFAEdge(q1, q2, p1[i]));

                q2.addDownstreamNode(q2);
                q2.addUpstreamNode(q2);
                dfaNodes.put(p1[i].hashCode(), new DFAEdge(q2, q2, p1[i]));

                q3.addDownstreamNode(q2);
                q2.addUpstreamNode(q3);
                dfaNodes.put(p2[i].hashCode(), new DFAEdge(q2, q3, p2[i]));

                q3.addDownstreamNode(q3);
                q3.addUpstreamNode(q3);
                dfaNodes.put(p2[i].hashCode(), new DFAEdge(q3, q3, p2[i]));


                InputTuple<Integer, Integer, Integer> input = stream.next();

                while (input != null) {
                    //retrieve DFA nodes where transition is same as edge label
                    Set<DFAEdge<String>> edges = dfaNodes.get(input.getLabel());
                    for (DFAEdge<String> edge : edges) {
//                    // for each such node, push tuple for processing at the target node
                        Tuple tuple = new Tuple(input.getSource(), input.getTarget(), edge.getSource().getNodeId());
                        edge.getSource().prepend(tuple, edge.getTarget().getNodeId());
                    }
                    // incoming edge fully processed, move to next one
                    input = stream.next();
                }

                System.out.println("total number of results for query " + queryNames[i] + " : " + q3.getResultCounter());

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
