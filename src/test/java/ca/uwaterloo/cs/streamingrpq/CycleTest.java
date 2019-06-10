package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.data.NoSpaceException;
import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;
import ca.uwaterloo.cs.streamingrpq.util.PathSemantics;
import com.google.common.collect.HashMultimap;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class CycleTest {

    static String filename = "src/main/resources/cycle.txt";

    public static void main(String[] args) {
        TextStream stream = new Yago2sTSVStream();

        DFA<Character> cycleTestDFA = new DFA(128, PathSemantics.SIMPLE);

        cycleTestDFA.addDFAEdge(0, 1, 'a');
        cycleTestDFA.addDFAEdge(1, 2, 'b');
        cycleTestDFA.addDFAEdge(2, 3, 'b');
        cycleTestDFA.addDFAEdge(3, 4, 'b');
        cycleTestDFA.addDFAEdge(4, 5, 'c');
        cycleTestDFA.setStartState(0);
        cycleTestDFA.setFinalState(5);

        stream.open(filename);
        InputTuple<Integer, Integer, Character> input = stream.next();

        while(input != null) {
            //retrieve DFA nodes where transition is same as edge label
            try {
                cycleTestDFA.processEdge(input);
            } catch (NoSpaceException e) {
                e.printStackTrace();
            }
            // incoming edge fully processed, move to next one
            input = stream.next();
        }

        // stream is over so we can close it and close the program
        System.out.println("total number of results: " + cycleTestDFA.getResultCounter());

        cycleTestDFA.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getLeft() + " --> " + t.getRight());});

        stream.close();

        HashMultimap<Integer, Integer> x = HashMultimap.create(10, 10);
        x.put(5, 6);
        x.put(5,7);
        x.put(5,7);
        x.put(4,null);
        x.put(5, null);
        x.put(4, 10);

    }
}
