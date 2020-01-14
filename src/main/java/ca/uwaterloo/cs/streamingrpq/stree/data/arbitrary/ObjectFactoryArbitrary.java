package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.engine.AbstractTreeExpansionJob;
import ca.uwaterloo.cs.streamingrpq.stree.engine.TreeNodeRAPQTreeExpansionJob;

import java.util.Queue;

public class ObjectFactoryArbitrary<V> implements ObjectFactory<V, SpanningTreeRAPQ<V>, TreeNode<V>> {
    @Override
    public TreeNode<V> createTreeNode(SpanningTreeRAPQ<V> tree, V vertex, int state, TreeNode<V> parentNode, long timestamp) {
        TreeNode<V> child = new TreeNode<V>(vertex, state, parentNode, tree, timestamp);
        return child;
    }

    @Override
    public SpanningTreeRAPQ<V> createSpanningTree(Delta<V, SpanningTreeRAPQ<V>, TreeNode<V>> delta, V vertex, long timestamp) {
        return new SpanningTreeRAPQ<V>(delta, vertex, timestamp);
    }

    @Override
    public <L> AbstractTreeExpansionJob createExpansionJob(ProductGraph<Integer, L> productGraph, QueryAutomata<L> automata, Queue<ResultPair<Integer>> results, boolean isDeletion) {
        TreeNodeRAPQTreeExpansionJob<L> expansionJob = new TreeNodeRAPQTreeExpansionJob<>(productGraph, automata, results, isDeletion);
        return expansionJob;
    }

}
