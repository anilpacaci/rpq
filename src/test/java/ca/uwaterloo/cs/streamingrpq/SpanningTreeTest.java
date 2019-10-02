package ca.uwaterloo.cs.streamingrpq;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.SimpleTextStream;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.input.Yago2sTSVStream;
import ca.uwaterloo.cs.streamingrpq.stree.data.QueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.engine.IncrementalRAPQ;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class SpanningTreeTest {

    static String filename = "src/main/resources/diamondgraph.txt";

    public static void main(String[] args) {
        QueryAutomata<String> query = new QueryAutomata<String>(3);
        query.addFinalState(2);
        query.addTransition(0, "a", 1);
        query.addTransition(1, "b", 2);
        query.addTransition(2, "a", 1);

        IncrementalRAPQ<String> rapqEngine = new IncrementalRAPQ(query);
        MetricRegistry metricRegistry = new MetricRegistry();
        rapqEngine.addMetricRegistry(metricRegistry);

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);


        TextStream stream = new SimpleTextStream();
        stream.open(filename);
        InputTuple<Integer, Integer, String> input = stream.next();

        while (input != null) {
            rapqEngine.processEdge(input);
            input = stream.next();
        }

        rapqEngine.getResults().entries().iterator().forEachRemaining(t-> {System.out.println(t.getKey() + " --> " + t.getValue());});

    }
}
