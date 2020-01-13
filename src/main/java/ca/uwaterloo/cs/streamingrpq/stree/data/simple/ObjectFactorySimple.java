package ca.uwaterloo.cs.streamingrpq.stree.data.simple;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractSpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractTreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.data.ObjectFactory;
import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;

public class ObjectFactorySimple<V> implements ObjectFactory<V> {
    @Override
    public TreeNodeRSPQ<V> createTreeNode(AbstractSpanningTree<V> tree, V vertex, int state, AbstractTreeNode<V> parentNode, long timestamp) {
        TreeNodeRSPQ<V> child = new TreeNodeRSPQ<V>(vertex, state, (TreeNodeRSPQ) parentNode, (SpanningTreeRSPQ<V>) tree, timestamp);
        return child;
    }

    @Override
    public SpanningTreeRSPQ<V> createSpanningTree(Delta<V> delta, V vertex, long timestamp) {
        return new SpanningTreeRSPQ<V>(delta, vertex, timestamp);
    }
}
