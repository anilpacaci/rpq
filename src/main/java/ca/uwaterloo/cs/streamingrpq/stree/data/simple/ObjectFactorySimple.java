package ca.uwaterloo.cs.streamingrpq.stree.data.simple;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.engine.AbstractTreeExpansionJob;
import ca.uwaterloo.cs.streamingrpq.stree.engine.TreeNodeRSPQTreeExpansionJob;
import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;

import java.util.Queue;
import java.util.Set;

public class ObjectFactorySimple<V> implements ObjectFactory<V, SpanningTreeRSPQ<V>, TreeNodeRSPQ<V>> {
    @Override
    public TreeNodeRSPQ<V> createTreeNode(SpanningTreeRSPQ<V> tree, V vertex, int state, TreeNodeRSPQ<V> parentNode, long timestamp) {
        TreeNodeRSPQ<V> child = new TreeNodeRSPQ<V>(vertex, state, (TreeNodeRSPQ) parentNode, (SpanningTreeRSPQ<V>) tree, timestamp);
        return child;
    }

    @Override
    public SpanningTreeRSPQ<V> createSpanningTree(Delta<V, SpanningTreeRSPQ<V>, TreeNodeRSPQ<V>> delta, V vertex, long timestamp) {
        return new SpanningTreeRSPQ<V>(delta, vertex, timestamp);
    }

    @Override
    public <L> AbstractTreeExpansionJob createExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results, boolean isDeletion) {
        TreeNodeRSPQTreeExpansionJob<L> expansionJob = new TreeNodeRSPQTreeExpansionJob<>(productGraph, automata, results, isDeletion);
        return expansionJob;
    }
}
