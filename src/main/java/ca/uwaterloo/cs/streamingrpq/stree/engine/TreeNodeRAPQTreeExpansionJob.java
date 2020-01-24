package ca.uwaterloo.cs.streamingrpq.stree.engine;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNodeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.query.Automata;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.Set;

public class TreeNodeRAPQTreeExpansionJob<L> extends AbstractTreeExpansionJob<L, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>{


    public TreeNodeRAPQTreeExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results, boolean isDeletion) {
        super(productGraph, automata, results, isDeletion);

        // initialize node types
        this.spanningTree = new SpanningTreeRAPQ[Constants.EXPECTED_BATCH_SIZE];
        this.parentNode = new TreeNodeRAPQ[Constants.EXPECTED_BATCH_SIZE];
    }


    @Override
    public Integer call() throws Exception {

        if(isDeletion) {
            // call each job in teh buffer
            for (int i = 0; i < currentSize; i++) {
                markExpired(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i]);
            }
        } else {
            // call each job in teh buffer
            for (int i = 0; i < currentSize; i++) {
                processTransition(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i]);
            }
        }
        return this.results.size();
    }

    @Override
    public void processTransition(SpanningTreeRAPQ<Integer> tree, TreeNodeRAPQ<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
        // either update timestamp, or create the node
        if(tree.exists(childVertex, childState)) {
            // if the child node already exists, we might need to update timestamp
            TreeNodeRAPQ<Integer> childNode = tree.getNodes(childVertex, childState).stream().findFirst().get();

            // root's children have timestamp equal to the edge timestamp
            // root timestmap always higher than any node in the tree
            if(parentNode.equals(tree.getRootNode())) {
                childNode.setTimestamp(edgeTimestamp);
                parentNode.setTimestamp( edgeTimestamp);
                // properly update the parent pointer
                childNode.setParent(parentNode);
            }
            // child node cannot be the root because parent has to be at least
            else if(childNode.getTimestamp() < Long.min(parentNode.getTimestamp(), edgeTimestamp)) {
                // only update its timestamp if there is a younger  path, back edge is guarenteed to be at smaller or equal
                childNode.setTimestamp(Long.min(parentNode.getTimestamp(), edgeTimestamp));
                // properly update the parent pointer
                childNode.setParent(parentNode);
            }

        } else {
            // extend the spanning tree with incoming node

            // root's children have timestamp equal to the edge timestamp
            // root timestmap always higher than any node in the tree
            TreeNodeRAPQ<Integer> childNode;
            if(parentNode.equals(tree.getRootNode())) {
                childNode = tree.addNode(parentNode, childVertex, childState, edgeTimestamp);
                parentNode.setTimestamp(edgeTimestamp);
            }
            else {
                childNode = tree.addNode(parentNode, childVertex, childState, Long.min(parentNode.getTimestamp(), edgeTimestamp));
            }
            // add this pair to results if it is a final state
            if (automata.isFinalState(childState)) {
                results.add(new ResultPair<>(tree.getRootVertex(), childVertex));
                resultCount++;
            }

            // get all the forward edges of the new extended node
            Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);

            if (forwardEdges == null) {
                // TODO better nul handling
                // end recursion if node has no forward edges
                return;
            } else {
                // there are forward edges, iterate over them
                for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                    // recursive call as the target of the forwardEdge has not been visited in state targetState before
                    //processTransition(tree, childNode, forwardEdge.getTarget(), targetState, forwardEdge.getTimestamp());
                    processTransition(tree, (TreeNodeRAPQ<Integer>) childNode, forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState(), forwardEdge.getTimestamp());
                }
            }
        }
    }

    @Override
    public void markExpired(SpanningTreeRAPQ<Integer> tree, TreeNodeRAPQ<Integer> parentNode, int childVertex, int childState, long edgeTimestamp) {
        // update the timestamp of the entire subtree of such node exists
        if(tree.exists(childVertex, childState)) {
            // if the child node already exists, we might need to update timestamp
            TreeNodeRAPQ<Integer> childNode = tree.getNodes(childVertex, childState).stream().findFirst().get();

            Queue<TreeNodeRAPQ<Integer>> queue = new ArrayDeque<>();
            queue.offer(childNode);
            while(!queue.isEmpty()) {
                TreeNodeRAPQ<Integer> currentNode = queue.poll();
                currentNode.setDeleted();
                queue.addAll(currentNode.getChildren());
            }

            // allnodes are marked,
            // simply call expiry on the spanning tree

            // it is OK to remove the deletion with timestamp 0 because we only want to delete nodes that are set to Long.MIN
            Collection<TreeNodeRAPQ<Integer>> removedNodes = tree.removeOldEdges(0, productGraph);

            for(TreeNodeRAPQ<Integer> removedNode : removedNodes) {
                if(automata.isFinalState(removedNode.getState())) {
                    results.add(new ResultPair<Integer>(tree.getRootVertex(), removedNode.getVertex(), true));
                    resultCount--;
                }
            }

        } else {
            // there is no such edge so no need for deletion
        }
    }
}
