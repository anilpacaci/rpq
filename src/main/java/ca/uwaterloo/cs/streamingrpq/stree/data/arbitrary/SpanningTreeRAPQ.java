package ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.simple.SpanningTreeRSPQ;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;

import java.util.*;

public class SpanningTreeRAPQ<V> extends AbstractSpanningTree<V, SpanningTreeRAPQ<V>, TreeNode<V>> {

    protected SpanningTreeRAPQ(Delta<V, SpanningTreeRAPQ<V>, TreeNode<V>> delta, V rootVertex, long timestamp) {
        super(timestamp, delta);

        TreeNode<V> root = new TreeNode<V>(rootVertex, 0, null, this, timestamp);
        this.rootNode = root;
        this.delta = delta;
        nodeIndex.put(Hasher.createTreeNodePairKey(rootVertex, 0), root);

        candidates = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        candidateRemoval = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        visited = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
    }

    @Override
    protected long populateCandidateRemovals(long minTimestamp) {
        // perform a bfs traversal on tree, no need for visited as it is a three
        LinkedList<TreeNode<V>> queue = new LinkedList<>();

        // minTimestamp of the tree should be updated, find the lowest timestamp in the tree higher than the minTimestmap
        // because after this maintenance, there is not going to be a node in the tree lower than the minTimestamp
        long minimumValidTimetamp = Long.MAX_VALUE;
        queue.addAll(rootNode.getChildren());

        while(!queue.isEmpty()) {
            // populate the queue with children
            TreeNode<V> currentVertex = queue.remove();
            queue.addAll(currentVertex.getChildren());

            // check time timestamp to decide whether it is expired
            if(currentVertex.getTimestamp() <= minTimestamp) {
                candidates.add(currentVertex);
            }
            // find minValidTimestamp for filtering for the next maintenance window
            if(currentVertex.getTimestamp() > minTimestamp && currentVertex.getTimestamp() < minimumValidTimetamp) {
                minimumValidTimetamp = currentVertex.getTimestamp();
            }
        }

        return minimumValidTimetamp;
    }

}
