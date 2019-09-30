package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;
import ca.uwaterloo.cs.streamingrpq.stree.data.Graph;
import ca.uwaterloo.cs.streamingrpq.stree.data.SpanningTree;

public class IncrementalRAPQ {

    private Delta<Integer> delta;
    private Graph<Integer, String> graph;

    public void processTransition(SpanningTree<Integer> tree,  int parentNode, int parentState, int childNode, int childState) {
        
    }
}
