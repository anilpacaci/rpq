package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractSpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractTreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;
import ca.uwaterloo.cs.streamingrpq.stree.data.ObjectFactory;

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
}
