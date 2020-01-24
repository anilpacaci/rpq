package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.engine.AbstractTreeExpansionJob;
import ca.uwaterloo.cs.streamingrpq.stree.engine.TreeNodeRAPQTreeExpansionJob;
import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;

import java.util.Queue;
import java.util.Set;

public class ObjectFactoryArbitrary<V> implements ObjectFactory<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> {
    @Override
    public TreeNodeRAPQ<V> createTreeNode(SpanningTreeRAPQ<V> tree, V vertex, int state, TreeNodeRAPQ<V> parentNode, long timestamp) {
        TreeNodeRAPQ<V> child = new TreeNodeRAPQ<V>(vertex, state, parentNode, tree, timestamp);
        return child;
    }

    @Override
    public SpanningTreeRAPQ<V> createSpanningTree(Delta<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> delta, V vertex, long timestamp) {
        return new SpanningTreeRAPQ<V>(delta, vertex, timestamp);
    }

    @Override
    public <L> AbstractTreeExpansionJob createExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results, boolean isDeletion) {
        TreeNodeRAPQTreeExpansionJob<L> expansionJob = new TreeNodeRAPQTreeExpansionJob<>(productGraph, automata, results, isDeletion);
        return expansionJob;
    }

}
