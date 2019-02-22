package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.core.Tuple;
import ca.uwaterloo.cs.streamingrpq.dfa.DFAEdge;
import ca.uwaterloo.cs.streamingrpq.dfa.DFANode;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import com.google.common.collect.HashMultimap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class CycleTest {

    static String filename = "src/main/resources/cycle.txt";

    public static void main(String[] args) {
        TextStream stream = new TextStream();

        DFANode q0 = new DFANode(0);
        DFANode q1 = new DFANode(1);
        DFANode q2 = new DFANode(2);
        DFANode q3 = new DFANode(3);
        DFANode q4 = new DFANode(4);
        DFANode q5 = new DFANode(5, true);


        HashMultimap<Character, DFAEdge<Character>> dfaNodes = HashMultimap.create();

        q0.addUpstreamNode(q1);
        dfaNodes.put('a', new DFAEdge(q0, q1, 'a'));

        q1.addUpstreamNode(q2);
        dfaNodes.put('b', new DFAEdge(q1, q2, 'b'));

        q2.addUpstreamNode(q3);
        dfaNodes.put('b', new DFAEdge(q2, q3, 'b'));

        q3.addUpstreamNode(q4);
        dfaNodes.put('b', new DFAEdge(q3, q4, 'b'));

        q4.addUpstreamNode(q5);
        dfaNodes.put('c', new DFAEdge(q4, q5, 'c'));

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
                }
                // incoming edge fully processed, move to next one
                input = stream.next();
            }

            // stream is over so we can close it and close the program
            System.out.println("total number of results: " + q5.getResultCounter());

            q5.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getSource() + " --> " + t.getTarget());});

            stream.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
