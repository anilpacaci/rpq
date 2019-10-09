package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SpanningTree<V> {

    private TreeNode<V> rootNode;
    private Delta<V> delta;

    Table<V, Integer, TreeNode> nodeIndex;

    private final Logger LOG = LoggerFactory.getLogger(SpanningTree.class);

    private long minTimestamp;

    protected SpanningTree(Delta<V> delta, V rootVertex, long timestamp) {
        this.rootNode = new TreeNode<V>(rootVertex, 0, null, this, timestamp);
        this.delta = delta;
        this.nodeIndex = HashBasedTable.create();
        nodeIndex.put(rootVertex, 0, rootNode);
        this.minTimestamp = timestamp;
    }


    public TreeNode<V> addNode(TreeNode parentNode, V childVertex, int childState, long timestamp) {
        if(parentNode == null) {
            // TODO no object found
        }
        if(parentNode.getTree().equals(this)) {
            // TODO wrong tree
        }

        TreeNode<V> child = new TreeNode<>(childVertex, childState, parentNode, this, timestamp);
        nodeIndex.put(childVertex, childState, child);

        // a new node is added to the spanning tree. update delta index
        this.delta.addToTreeNodeIndex(this, child);

        this.updateTimestamp(timestamp);

        return child;
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.contains(vertex, state);
    }

    public TreeNode getNode(V vertex, int state) {
        TreeNode node = nodeIndex.get(vertex, state);
        return node;
    }

    public V getRootVertex() {
        return this.rootNode.getVertex();
    }

    public TreeNode<V> getRootNode() {
        return this.rootNode;
    }

    protected void updateTimestamp(long timestamp) {
        if(timestamp < minTimestamp) {
            this.minTimestamp = timestamp;
        }
    }

    protected long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * removes old edges from the productGraph, used during window management.
     * This function assumes that expired edges are removed from the productGraph, so traversal assumes that it is guarenteed to
     * traverse valid edges
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     * @return The set of nodes that have expired from the window as there is no other path
     */
    protected <L> Collection<TreeNode<V>> removeOldEdges(long minTimestamp, ProductGraph<V,L> productGraph) {
        // if root is expired (root node timestamp is its youngest edge), then the entire tree needs to be removed
//        if(this.rootNode.getTimestamp() <= minTimestamp) {
//            return this.nodeIndex.values();
//        }

        // potentially expired nodes
        HashSet<TreeNode<V>> candidates = new HashSet<>();
        HashSet<TreeNode<V>> candidateRemoval = new HashSet<>();

        // perform a bfs traversal on tree, no need for visited as it is a three
        LinkedList<TreeNode> queue = new LinkedList<>();
        // minTimestamp of the tree should be updated, find the lowest timestamp in the tree higher than the minTimestmap
        // because after this maintenance, there is not going to be a node in the tree lower than the minTimestamp
        long minimumValidTimetamp = Long.MAX_VALUE;
        queue.addAll(rootNode.getChildren());
        while(!queue.isEmpty()) {
            // populate the queue with children
            TreeNode currentVertex = queue.remove();
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
        //update the lowest minimum timestamp for this tree
        this.minTimestamp = minimumValidTimetamp;

        Iterator<TreeNode<V>> candidateIterator = candidates.iterator();
        HashSet<TreeNode> visited = new HashSet<>();

        LOG.debug("Expiry for spanning tree {}, # of candidates {} out of {} nodes", toString(), candidates.size(), nodeIndex.size());

        //scan over potential nodes once.
        // For each potential, check they have a valid non-tree edge in the original productGraph
        // If there is traverse down from here (in the productGraph) and remove all children from potentials
        while(candidateIterator.hasNext()) {
            TreeNode<V> candidate = candidateIterator.next();
            // check if a previous traversal already found a path for the candidate
            if(candidate.getTimestamp() > minTimestamp) {
                continue;
            }
            //check if there exists any incoming edge from a valid state
            Collection<GraphEdge<ProductGraphNode<V>>> backwardEdges = productGraph.getBackwardEdges(candidate.getVertex(), candidate.getState());
            TreeNode<V> newParent = null;
            GraphEdge<ProductGraphNode<V>> newParentEdge = null;
            for(GraphEdge<ProductGraphNode<V>> backwardEdge : backwardEdges) {
                newParent = this.getNode(backwardEdge.getSource().getVertex(), backwardEdge.getSource().getState());
                if (newParent != null && (!candidates.contains(newParent) || candidateRemoval.contains(newParent))) {
                    // there is an incoming edge with valid source
                    // source is valid (in the tree) and not in candidate
                    newParentEdge = backwardEdge;
                    break;
                }
            }

            //if this node becomes valid, just traverse everything down in the productGraph. If somehow I traverse an edge who would
            // be an incoming edge of some candidate, then it is removed from candidate so I never check it there.
            // If that edge is checked during incoming edge search, than it might be only examined again with a traversal which makes sure
            // that edge cannot be visited again. Therefore it is O(m)
            if(newParentEdge != null) {
                // means that there was a tree node that is not in the candidates but in the tree as a valid node
                candidate.setParent(newParent);
                candidate.setTimestamp(Long.min(newParent.getTimestamp(), newParentEdge.getTimestamp()));
                // current vertex has a valid incoming edge, so it needs to be removed from candidates
                candidateRemoval.add(candidate);

                //now traverse the productGraph down from this node, and remove any visited node from candidates
                LinkedList<TreeNode> traversalQueue = new LinkedList<TreeNode>();

                // initial node is the current candidate, because now it is reachable
                traversalQueue.add(candidate);
                while(!traversalQueue.isEmpty()){
                    TreeNode<V> currentVertex = traversalQueue.remove();
                    visited.add(currentVertex);

                    Collection<GraphEdge<ProductGraphNode<V>>> forwardEdges = productGraph.getForwardEdges(currentVertex.getVertex(), currentVertex.getState());
                    // for each potential child
                    for(GraphEdge<ProductGraphNode<V>> forwardEdge : forwardEdges) {
                        // I can simply retrieve from the tree index because any node that is reachable are in tree index
                        TreeNode<V> outgoingTreeNode = this.getNode(forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState());
                        // there exists such node in the tree & the edge we are traversing is valid & this node has not been visited before
                        if (outgoingTreeNode != null && forwardEdge.getTimestamp() > minTimestamp && !visited.contains(outgoingTreeNode)) {
                            if (candidates.contains(outgoingTreeNode)) {
                                // remove this node from potentials as now there is a younger path
                                candidateRemoval.add(outgoingTreeNode);
                            }
                            if (outgoingTreeNode.getTimestamp() < Long.min(currentVertex.getTimestamp(), forwardEdge.getTimestamp())) {
                                // note anything in the candidates has a lower timestamp then
                                // min(currentVertex, forwardEdge) as currentVertex and forward edge are guarenteed to be larger than minTimestamp
                                outgoingTreeNode.setParent(currentVertex);
                                outgoingTreeNode.setTimestamp(Long.min(currentVertex.getTimestamp(), forwardEdge.getTimestamp()));
                                traversalQueue.add(outgoingTreeNode);
                            }
                        }
                        // nodes with forward edge smaller than minTimestamp with already younger paths no need to be visited
                        // so no need to add them into the queue
                    }
                }

            }

        }

        // update candidates
        candidates.removeAll(candidateRemoval);

        // now if there is any potential remanining, it is guarenteed that they are not reachable
        // so simply clean the indexes and generate negative result if necessary
        for(TreeNode<V> currentVertex : candidates) {
            // remove this node from the node index
            nodeIndex.remove(currentVertex.getVertex(), currentVertex.getState());
            //remove this node from parent's chilren list
            currentVertex.setParent(null);
        }

        LOG.debug("Spanning tree rooted at {}, remove {} nodes at timestamp {} ", getRootVertex(), candidates.size(), minTimestamp);

        // return all the remaining nodes, which have actually expired from the window
        return candidates;
    }

    /**
     * Checks whether the entire three has expired, i.e. there is no active edge from the root node
     * @return <code>true</> if there is no active edge from the root node
     */
    public boolean isExpired(long minTimestamp) {
        boolean expired = rootNode.getChildren().stream().allMatch(c -> c.getTimestamp() <= minTimestamp);
        return expired;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Root:").append(rootNode.getVertex()).append("-TS:").append(rootNode.getTimestamp()).toString();
    }

}
