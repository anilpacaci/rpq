package ca.uwaterloo.cs.streamingrpq.stree.data;

public interface  ObjectFactory<V> {

    AbstractTreeNode<V> createTreeNode(AbstractSpanningTree<V> tree, V vertex, int state, AbstractTreeNode<V> parentNode, long timestamp);

    AbstractSpanningTree<V> createSpanningTree(Delta<V> delta, V vertex, long timestamp);
}
