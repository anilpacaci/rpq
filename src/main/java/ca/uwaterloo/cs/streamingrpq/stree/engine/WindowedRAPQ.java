package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;
import ca.uwaterloo.cs.streamingrpq.stree.data.Graph;
import ca.uwaterloo.cs.streamingrpq.stree.data.QueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.data.SpanningTree;
import com.google.common.collect.HashMultimap;

/**
 * Created by anilpacaci on 2019-10-02.
 */
public class WindowedRAPQ<L> extends RPQEngine<L> {

    private int windowSize;
    private int slideSize;

    public WindowedRAPQ(QueryAutomata<L> query, int capacity, int windowSize, int slideSize) {
        super(query, capacity);
        this.windowSize = windowSize;
        this.slideSize = slideSize;
    }

    @Override
    public void processEdge(InputTuple<Integer, Integer, L> inputTuple) {

    }

    @Override
    public void processTransition(SpanningTree<Integer> tree, int parentVertex, int parentState, int childVertex, int childState) {

    }
}
