package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.core.Tuple;
import ca.uwaterloo.cs.streamingrpq.dfa.DFAEdge;
import ca.uwaterloo.cs.streamingrpq.dfa.DFANode;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sInMemoryTSVStream;
import com.google.common.collect.HashMultimap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class WaveGuideQ5 {

//    static String filename = "/Volumes/RAM Disk/xaa";
    static String filename = "/mnt/ramdisk/yago2s_full_virtuoso.csv";

    private static String p0 = "http://yago-knowledge.org/resource/isCitizenOf";
    private static String p1 = "http://yago-knowledge.org/resource/hasCapital";
    private static String p2 = "http://yago-knowledge.org/resource/participatedIn";

    public static void main(String[] args) {
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
