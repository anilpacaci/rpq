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
                        .configure(params.properties().setFileName(args[0]));

        Configuration config = null;

        try {
            config = builder.getConfiguration();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        if( !(config.containsKey("input.file") && config.containsKey("label.p") && config.containsKey("label.p1") && config.containsKey("label.p2")) ) {
            // parameters file does not have all the required parameters
            System.err.println("Parameters file does not have all required parameters");
            return;
        }

        String filename = config.getString("input.file");
        String p0 = config.getString("label.p");
        String p1 = config.getString("label.p1");
        String p2 = config.getString("label.p2");

        Yago2sInMemoryTSVStream stream = new Yago2sInMemoryTSVStream();

        DFANode q0 = new DFANode(0);
        DFANode q1 = new DFANode(1);
        DFANode q2 = new DFANode(2);
        DFANode q3 = new DFANode(3, true);


        HashMultimap<Integer, DFAEdge<String>> dfaNodes = HashMultimap.create();

        q0.addUpstreamNode(q1);
        dfaNodes.put(p0.hashCode(), new DFAEdge(q0, q1, p0));

        q1.addDownstreamNode(q0);
        q1.addUpstreamNode(q2);
        dfaNodes.put(p1.hashCode(), new DFAEdge(q1, q2, p1));

        q2.addDownstreamNode(q2);
        q2.addUpstreamNode(q2);
        dfaNodes.put(p1.hashCode(), new DFAEdge(q2, q2, p1));

        q3.addDownstreamNode(q2);
        q2.addUpstreamNode(q3);
        dfaNodes.put(p2.hashCode(), new DFAEdge(q2, q3, p2));

        q3.addDownstreamNode(q3);
        q3.addUpstreamNode(q3);
        dfaNodes.put(p2.hashCode(), new DFAEdge(q3, q3, p2));


        try {
            stream.open(filename);
            InputTuple<Integer, Integer, Integer> input = stream.next();

            while(input != null) {
                //retrieve DFA nodes where transition is same as edge label
                Set<DFAEdge<String>> edges = dfaNodes.get(input.getLabel());
                for(DFAEdge<String> edge : edges) {
//                    // for each such node, push tuple for processing at the target node
                    Tuple tuple = new Tuple(input.getSource(), input.getTarget(), edge.getSource().getNodeId());
                    edge.getSource().prepend(tuple, edge.getTarget().getNodeId());
                }
                // incoming edge fully processed, move to next one
                input = stream.next();
            }

            // stream is over so we can close it and close the program
            stream.close();

            System.out.println("total number of results: " + q3.getResultCounter());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
