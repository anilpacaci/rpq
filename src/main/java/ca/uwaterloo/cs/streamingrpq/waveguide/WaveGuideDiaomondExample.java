package ca.uwaterloo.cs.streamingrpq.waveguide;

import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class WaveGuideDiaomondExample {

    static String filename = "src/main/resources/diamondgraph.txt";

    public static void main(String[] args) {
        TextStream stream = new Yago2sTSVStream();


        DFA<String> diamond = new DFA<>();
        diamond.addDFAEdge(0,1,"a");
        diamond.addDFAEdge(1,2,"b");
        diamond.addDFAEdge(2,1,"a");
        diamond.setStartState(0);
        diamond.setFinalState(2);
        diamond.optimize();

        stream.open(filename);
        InputTuple<Integer, Integer, String> input = stream.next();

        while(input != null) {
            diamond.processEdge(input);
            // incoming edge fully processed, move to next one
            input = stream.next();
        }

        // stream is over so we can close it and close the program
        System.out.println("total number of results: " + diamond.getResultCounter());
        System.out.println("Edges: " + diamond.getGraphEdgeCount());
        System.out.println("Delta: " + diamond.getDeltaTupleCount());

        diamond.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getSource() + " --> " + t.getTarget());});

        stream.close();


    }
}
