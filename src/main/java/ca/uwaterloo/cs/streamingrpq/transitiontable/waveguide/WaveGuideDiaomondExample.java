package ca.uwaterloo.cs.streamingrpq.transitiontable.waveguide;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.SimpleTextStream;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.transitiontable.data.NoSpaceException;
import ca.uwaterloo.cs.streamingrpq.transitiontable.dfa.DFA;
import ca.uwaterloo.cs.streamingrpq.transitiontable.util.PathSemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class WaveGuideDiaomondExample {

    private static Logger logger = LoggerFactory.getLogger(WaveGuideDiaomondExample.class);


    static String filename = "src/main/resources/diamondgraph.txt";

    public static void main(String[] args) {
        TextStream stream = new SimpleTextStream();


        DFA<String> diamond = new DFA<>(256, PathSemantics.fromValue("arbitrary"));
        diamond.addDFAEdge(0,1,"a");
        diamond.addDFAEdge(1,2,"b");
        diamond.addDFAEdge(2,1,"a");
        diamond.setStartState(0);
        diamond.setFinalState(2);
        diamond.optimize();

        stream.open(filename);
        InputTuple<Integer, Integer, String> input = stream.next();

        while(input != null) {
            try {
                diamond.processEdge(input);
            } catch (NoSpaceException e) {
                logger.error("SimpleDFST cannot grow", e);
            }
            // incoming edge fully processed, move to next one
            input = stream.next();
        }

        // stream is over so we can close it and close the program
        logger.info("total number of results: " + diamond.getResultCounter());
        logger.info("Edges: " + diamond.getGraphEdgeCount());
        logger.info("ArbitraryDFST: " + diamond.getDeltaTupleCount());

        diamond.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getLeft() + " --> " + t.getRight());});

        stream.close();


    }
}
