package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.SimpleTextStreamWithExplicitDeletions;
import ca.uwaterloo.cs.streamingrpq.input.TextFileStream;
import ca.uwaterloo.cs.streamingrpq.stree.data.ManualQueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNodeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.engine.RPQEngine;
import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRPQ;
import ca.uwaterloo.cs.streamingrpq.stree.query.BricsAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.query.BricsAutomataBuilder;
import ca.uwaterloo.cs.streamingrpq.stree.util.Semantics;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class WindowedSpanningTreeTest {

    static String filename = "src/main/resources/diamondgraph.txt";

    static String queryString = "ASK { { ?x1 (<a>/<b>)/(<a>/<b>)* ?x2. } }";

    public static void main(String[] args) {
        ManualQueryAutomata<String> query = new ManualQueryAutomata<String>(3);
        query.addFinalState(2);
        query.addTransition(0, "a", 1);
        query.addTransition(1, "b", 2);
        query.addTransition(2, "a", 1);

        BricsAutomataBuilder builder = new BricsAutomataBuilder();
        BricsAutomata bricsQuery = builder.fromSPARQL(queryString);
        bricsQuery.finalize();

        RPQEngine<String> rapqEngine = new WindowedRPQ<String, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>(bricsQuery, 100, 5, 1, 10, Semantics.ARBITRARY);
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

        rapqEngine.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getSource() + " --> " + t.getTarget() + " " + !t.isDeletion());});

        rapqEngine.shutDown();

        stream.close();

        reporter.stop();

    }
}
