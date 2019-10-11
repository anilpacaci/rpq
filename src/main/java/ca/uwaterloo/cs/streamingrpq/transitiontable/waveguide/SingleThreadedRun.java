package ca.uwaterloo.cs.streamingrpq.transitiontable.waveguide;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import ca.uwaterloo.cs.streamingrpq.stree.engine.RPQEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Created by anilpacaci on 2019-06-07.
 */
public class SingleThreadedRun<L> implements Callable {

    private static Logger logger = LoggerFactory.getLogger(SingleThreadedRun.class);


    private String queryName;
    private TextStream stream;
    private RPQEngine<L> query;

    public SingleThreadedRun(String queryName, TextStream stream, RPQEngine<L> query) {
        this.queryName = queryName;
        this.stream = stream;
        this.query = query;
    }

    @Override
    public Object call() throws Exception {

        InputTuple<Integer, Integer, L> input = stream.next();
        logger.info("Query " + queryName + " is starting!");

        while (input != null) {
            //retrieve DFA nodes where transition is same as edge label
            query.processEdge(input);
            // incoming edge fully processed, move to next one
            input = stream.next();

            if(Thread.currentThread().isInterrupted()) {
                logger.info("Query " + queryName + " is interrupted");
                return null;
            }
        }

        logger.info("total number of results for query " + queryName + " : " + query.getResultCount());
        return null;
    }


}
