package ca.uwaterloo.cs.streamingrpq.stree.data.simple;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.Delta;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SpanningTreeRSPQ<V> extends AbstractSpanningTree<V, SpanningTreeRSPQ<V>, TreeNodeRSPQ<V>> {

    private HashSet<Hasher.MapKey<V>> markings;

    private final Logger LOG = LoggerFactory.getLogger(SpanningTreeRSPQ.class);

    protected SpanningTreeRSPQ(Delta<V, SpanningTreeRSPQ<V>, TreeNodeRSPQ<V>> delta, V rootVertex, long timestamp) {
        super(timestamp, delta);

        this.rootNode = new TreeNodeRSPQ<V>(rootVertex, 0, null, this, timestamp);
        this.nodeIndex = HashMultimap.create(Constants.EXPECTED_TREE_SIZE, Constants.EXPECTED_LABELS);
        nodeIndex.put(Hasher.createTreeNodePairKey(rootVertex, 0), rootNode);
        this.markings = Sets.newHashSet();

        candidates = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        candidateRemoval = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        visited = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
    }

    public void addMarking(V vertex, int state) {
        markings.add(Hasher.createTreeNodePairKey(vertex, state));
    }

    public boolean isMarked(V vertex, int state) {
        return markings.contains(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    public void removeMarking(V vertex, int state) {
        markings.remove(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    /**
     * Implements unmarking procedure for nodes with conflicts
     * @param productGraph
     * @param parentNode
     */
    protected <L> void unmark(ProductGraph<V, L> productGraph, TreeNodeRSPQ<V> parentNode) {

        Stack<TreeNodeRSPQ<V>> unmarkedNodes = new Stack<>();

        TreeNodeRSPQ<V> currentNode = parentNode;

        // first unmark all marked nodes upto parent
        while (currentNode != null && this.isMarked(currentNode.getVertex(), currentNode.getState())) {
            this.removeMarking(currentNode.getVertex(), currentNode.getState());
            unmarkedNodes.push(currentNode);
            currentNode = currentNode.getParent();
        }

        // now simply traverse back edges of the candidates and invoke processTransition
        while (!unmarkedNodes.isEmpty()) {
            currentNode =  unmarkedNodes.pop();

            // get backward edges of the unmarked node
            Collection<GraphEdge<ProductGraphNode<V>>> backwardEdges = productGraph.getBackwardEdges(currentNode.getVertex(), currentNode.getState());
            for (GraphEdge<ProductGraphNode<V>> backwardEdge : backwardEdges) {
                V sourceVertex = backwardEdge.getSource().getVertex();
                int sourceState = backwardEdge.getSource().getState();
                // find all the nodes that are pruned due to previously marking
                Collection<TreeNodeRSPQ<V>> parentNodes = this.getNodes(sourceVertex, sourceState);
                for (TreeNodeRSPQ<V> p : parentNodes) {
                    // force casting
                    TreeNodeRSPQ<V> parent =  p;
                    // try to extend if it is not a cycle in the product graph
                    if (!parent.containsCM(currentNode.getVertex(), currentNode.getState())) {
                        extendPrefixPath(productGraph, parent, currentNode.getVertex(), currentNode.getState(), backwardEdge.getTimestamp());
                    }
                }
            }
        }
    }

    protected <L> void extendPrefixPath(ProductGraph<V, L> productGraph, TreeNodeRSPQ<V> parentNode, V targetVertex, int targetState, long edgeTimestamp) {

    }

    @Override
    protected long populateCandidateRemovals(long minTimestamp) {
        // perform a bfs traversal on tree, no need for visited as it is a three
        LinkedList<TreeNodeRSPQ<V>> queue = new LinkedList<>();
        // minTimestamp of the tree should be updated, find the lowest timestamp in the tree higher than the minTimestmap
        // because after this maintenance, there is not going to be a node in the tree lower than the minTimestamp
        long minimumValidTimetamp = Long.MAX_VALUE;
        queue.addAll(rootNode.getChildren());
        while(!queue.isEmpty()) {
            // populate the queue with children
            TreeNodeRSPQ<V> currentVertex = queue.remove();
            queue.addAll(currentVertex.getChildren());

            // check time timestamp to decide whether it is expired
            if(currentVertex.getTimestamp() <= minTimestamp) {
                // if node is unmarked simply remove it here
                if(this.isMarked(currentVertex.getVertex(), currentVertex.getState())) {
                    // unmarked nodes are guarenteed to have no cross-edges
                    this.removeNode(currentVertex);
                } else {
                    // marked nodes are just candidate for removal
                    candidates.add(currentVertex);
                }

            }
            // find minValidTimestamp for filtering for the next maintenance window
            if(currentVertex.getTimestamp() > minTimestamp && currentVertex.getTimestamp() < minimumValidTimetamp) {
                minimumValidTimetamp = currentVertex.getTimestamp();
            }
        }

        return minimumValidTimetamp;
    }
}
