package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.core.Tuple;
import ca.uwaterloo.cs.streamingrpq.dfa.DFAEdge;
import ca.uwaterloo.cs.streamingrpq.dfa.DFANode;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import org.antlr.v4.runtime.misc.MultiMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class WaveGuideDiaomondExample {

    static String filename = "src/main/resources/diamondgraph.txt";

    public static void main(String[] args) {
        TextStream stream = new TextStream();

        DFANode q0 = new DFANode(0);
        DFANode q1 = new DFANode(1);
        DFANode q2 = new DFANode(2);

        HashMultimap<Character, DFAEdge<Character>> dfaNodes = HashMultimap.create();

        q0.addUpstreamNode(q1);
        dfaNodes.put('a', new DFAEdge(q0, q1, 'a'));

        q1.addDownstreamNode(q0);
        q1.addUpstreamNode(q2);
        dfaNodes.put('b', new DFAEdge(q1, q2, 'b'));

        q2.addDownstreamNode(q1);
        q2.addUpstreamNode(q1);
        dfaNodes.put('a', new DFAEdge(q2, q1, 'a'));

        try {
            stream.open(filename);
            InputTuple<Integer, Integer, Character> input = stream.next();

            while(input != null) {
                //retrieve DFA nodes where transition is same as edge label
                Set<DFAEdge<Character>> edges = dfaNodes.get(input.getLabel());
                for(DFAEdge<Character> edge : edges) {
                    // for each such node, push tuple for processing at the target node
                    Tuple tuple = new Tuple(input.getSource(), input.getTarget(), edge.getSource().getNodeId());
                    edge.getSource().prepend(tuple, edge.getTarget().getNodeId());
                    edge.getTarget().process(tuple);
                }
                // incoming edge fully processed, move to next one
                input = stream.next();
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
