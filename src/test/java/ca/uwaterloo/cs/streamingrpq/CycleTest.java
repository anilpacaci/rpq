package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.core.SubPath;
import ca.uwaterloo.cs.streamingrpq.dfa.DFA;
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

        DFA<Character> cycleTestDFA = new DFA();

        cycleTestDFA.addDFAEdge(0, 1, 'a');
        cycleTestDFA.addDFAEdge(1, 2, 'b');
        cycleTestDFA.addDFAEdge(2, 3, 'b');
        cycleTestDFA.addDFAEdge(3, 4, 'b');
        cycleTestDFA.addDFAEdge(4, 5, 'c');
        cycleTestDFA.setFinalState(5);

        try {
            stream.open(filename);
            InputTuple<Integer, Integer, Character> input = stream.next();

            while(input != null) {
                //retrieve DFA nodes where transition is same as edge label
                cycleTestDFA.processEdge(input);
                // incoming edge fully processed, move to next one
                input = stream.next();
            }

            // stream is over so we can close it and close the program
            System.out.println("total number of results: " + cycleTestDFA.getResultCounter());

            cycleTestDFA.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getSource() + " --> " + t.getTarget());});

            stream.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
