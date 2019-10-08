package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.SpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.TreeNode;

public class TreeExpansionJob {

    private SpanningTree<Integer> spanningTree;
    private TreeNode<Integer> parentNode;
    private int targetVertex;
    private int targetState;
    private long edgeTimestamp;

    public TreeExpansionJob(SpanningTree<Integer> spanningTree, TreeNode<Integer> parentNode, int targetVertex, int targetState, long edgeTimestamp) {
        this.spanningTree = spanningTree;
        this.parentNode = parentNode;
        this.targetVertex = targetVertex;
        this.targetState = targetState;
        this.edgeTimestamp = edgeTimestamp;
    }

    public SpanningTree<Integer> getSpanningTree() {
        return spanningTree;
    }

    public TreeNode<Integer> getParentNode() {
        return parentNode;
    }

    public int getTargetVertex() {
        return targetVertex;
    }

    public int getTargetState() {
        return targetState;
    }

    public long getEdgeTimestamp() {
        return edgeTimestamp;
    }
}
