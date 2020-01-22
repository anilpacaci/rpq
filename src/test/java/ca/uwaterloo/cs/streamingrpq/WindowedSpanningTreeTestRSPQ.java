package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.SimpleTextStreamWithExplicitDeletions;
import ca.uwaterloo.cs.streamingrpq.input.TextFileStream;
import ca.uwaterloo.cs.streamingrpq.stree.data.ManualQueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.engine.RPQEngine;
import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRPQ;
import ca.uwaterloo.cs.streamingrpq.stree.util.Semantics;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class WindowedSpanningTreeTestRSPQ {

    static String filename = "src/main/resources/examplegraph.txt";

    public static void main(String[] args) {
        ManualQueryAutomata<String> query = new ManualQueryAutomata<String>(3);
        query.addFinalState(0);
        query.addFinalState(1);
        query.addFinalState(2);
        query.addTransition(0, "m", 0);
        query.addTransition(0, "f", 1);
        query.addTransition(1, "f", 1);
        query.addTransition(1, "m", 2);
        query.addTransition(2, "m", 2);

        RPQEngine<String> rapqEngine = new WindowedRPQ<>(query, 100, 10, 1, 10,  Semantics.SIMPLE);
        MetricRegistry metricRegistry = new MetricRegistry();
        rapqEngine.addMetricRegistry(metricRegistry);

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);


        TextFileStream stream = new SimpleTextStreamWithExplicitDeletions();
        stream.open(filename);
        InputTuple<Integer, Integer, String> input = stream.next();

        while (input != null) {
            rapqEngine.processEdge(input);
            input = stream.next();
        }

        rapqEngine.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getSource() + " --> " + t.getTarget());});

        rapqEngine.shutDown();

        stream.close();

        reporter.stop();

    }
}
