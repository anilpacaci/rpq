package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.SpanningTreeRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.TreeNodeRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

public class TreeNodeRSPQTreeExpansionJob<L> extends AbstractTreeExpansionJob<L, SpanningTreeRSPQ<Integer>, TreeNodeRSPQ<Integer>> {

    public TreeNodeRSPQTreeExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results, boolean isDeletion) {
        super(productGraph, automata, results, isDeletion);

        this.spanningTree = new SpanningTreeRSPQ[Constants.EXPECTED_BATCH_SIZE];
        this.parentNode = new TreeNodeRSPQ[Constants.EXPECTED_BATCH_SIZE];
    }

    public TreeNodeRSPQTreeExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results) {
        this(productGraph, automata, results, false);
    }


    @Override
    public Integer call() throws Exception {

        // call each job in teh buffer
        for (int i = 0; i < currentSize; i++) {
            if (!parentNode[i].containsCM(targetVertex[i], targetState[i]) && !spanningTree[i].isMarked(targetVertex[i], targetState[i])) {
                processTransition(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i]);
            }
        }

        return this.results.size();
    }

    @Override
    public void processTransition(SpanningTreeRSPQ<Integer> tree, TreeNodeRSPQ<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
        if (!automata.hasContainment(parentNode.getFirstCM(childVertex), childState)) {
            // detected conflict parent node needs to be unmarked
            unmark(tree, parentNode);
        } else {
            // if this is the first target product node is visited, simply add it to the markings
            if (!tree.exists(childVertex, childState)) {
                tree.addMarking(childVertex, childState);
            }

            //add new node to the tree as a new child
            TreeNodeRSPQ<Integer> childNode;
            if (parentNode.equals(tree.getRootNode())) {
                childNode = tree.addNode(parentNode, childVertex, childState, edgeTimestamp);
                parentNode.setTimestamp(edgeTimestamp);
            } else {
                childNode = tree.addNode(parentNode, childVertex, childState, Long.min(parentNode.getTimestamp(), edgeTimestamp));
            }

            if (automata.isFinalState(childState)) {
                results.add(new ResultPair<>(tree.getRootVertex(), childVertex));
                resultCount++;
            }

            // get all the forward edges of the new extended node
            Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);

            if (forwardEdges == null) {
                return;
            } else {
                // there are forward edges, iterate over them
                for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                    int targetVertex = forwardEdge.getTarget().getVertex();
                    int targetState = forwardEdge.getTarget().getState();
                    if (!childNode.containsCM(targetVertex, targetState) && !tree.isMarked(targetVertex, targetState)) {
                        // visit a node only if that same node is not visited at the same state before
                        // simply prevent cycles in product graph
                        processTransition(tree, childNode, targetVertex, targetState, forwardEdge.getTimestamp());
                    }
                }
            }
        }
    }

    /**
     * Implements unmarking procedure for nodes with conflicts
     *
     * @param tree
     * @param parentNode
     */
    public void unmark(SpanningTreeRSPQ<Integer> tree, TreeNodeRSPQ<Integer> parentNode) {

        Stack<TreeNodeRSPQ<Integer>> unmarkedNodes = new Stack<>();

        TreeNodeRSPQ<Integer> currentNode = parentNode;

        // first unmark all marked nodes upto parent
        while (currentNode != null && tree.isMarked(currentNode.getVertex(), currentNode.getState())) {
            tree.removeMarking(currentNode.getVertex(), currentNode.getState());
            unmarkedNodes.push(currentNode);
            currentNode = currentNode.getParent();
        }

        // now simply traverse back edges of the candidates and invoke processTransition
        while (!unmarkedNodes.isEmpty()) {
            currentNode = unmarkedNodes.pop();

            // get backward edges of the unmarked node
            Collection<GraphEdge<ProductGraphNode<Integer>>> backwardEdges = productGraph.getBackwardEdges(currentNode.getVertex(), currentNode.getState());
            for (GraphEdge<ProductGraphNode<Integer>> backwardEdge : backwardEdges) {
                int sourceVertex = backwardEdge.getSource().getVertex();
                int sourceState = backwardEdge.getSource().getState();
                // find all the nodes that are pruned due to previously marking
                Collection<TreeNodeRSPQ<Integer>> parentNodes = tree.getNodes(sourceVertex, sourceState);
                for (TreeNodeRSPQ<Integer> parent : parentNodes) {
                    // try to extend if it is not a cycle in the product graph
                    if (!parent.containsCM(currentNode.getVertex(), currentNode.getState())) {
                        processTransition(tree, parent, currentNode.getVertex(), currentNode.getState(), backwardEdge.getTimestamp());
                    }
                }
            }
        }

    }

    @Override
    public void markExpired(SpanningTreeRSPQ<Integer> tree, TreeNodeRSPQ<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
        //TODO implement deletion for simple path semantics
    }
}