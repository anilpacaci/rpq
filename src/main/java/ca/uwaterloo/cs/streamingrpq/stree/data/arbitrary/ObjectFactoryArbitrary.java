package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractSpanningTree;
import ca.uwaterloo.cs.streamingrpq.stree.data.AbstractTreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;
import ca.uwaterloo.cs.streamingrpq.stree.data.ObjectFactory;

public class ObjectFactoryArbitrary<V> implements ObjectFactory<V> {
    @Override
    public TreeNode<V> createTreeNode(AbstractSpanningTree<V> tree, V vertex, int state, AbstractTreeNode<V> parentNode, long timestamp) {
        TreeNode<V> child = new TreeNode<V>(vertex, state, (TreeNode) parentNode, (SpanningTreeRAPQ<V>) tree, timestamp);
        return child;
    }

    @Override
    public SpanningTreeRAPQ<V> createSpanningTree(Delta<V> delta, V vertex, long timestamp) {
        return new SpanningTreeRAPQ<V>(delta, vertex, timestamp);
    }
}
