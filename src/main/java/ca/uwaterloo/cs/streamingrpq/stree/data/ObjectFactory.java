package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.engine.AbstractTreeExpansionJob;

import java.util.Queue;

public interface  ObjectFactory<V, T extends AbstractSpanningTree<V, T, N>, N extends AbstractTreeNode<V, T, N>> {

    N createTreeNode(T tree, V vertex, int state, N parentNode, long timestamp);

    T createSpanningTree(Delta<V, T, N> delta, V vertex, long timestamp);

    <L> AbstractTreeExpansionJob createExpansionJob(ProductGraph<Integer,L> productGraph, ManualQueryAutomata<L> automata, Queue<ResultPair<Integer>> results, boolean isDeletion);
}
