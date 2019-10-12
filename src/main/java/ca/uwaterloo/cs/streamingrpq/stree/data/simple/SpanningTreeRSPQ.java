package ca.uwaterloo.cs.streamingrpq.stree.data.simple;

import ca.uwaterloo.cs.streamingrpq.stree.data.*;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SpanningTreeRSPQ<V> {

    private TreeNodeRSPQ<V> rootNode;
    private DeltaRSPQ<V> delta;

    private Multimap<Hasher.MapKey<V>, TreeNodeRSPQ<V>> nodeIndex;

    private HashSet<Hasher.MapKey<V>> markings;

    private final Logger LOG = LoggerFactory.getLogger(SpanningTreeRSPQ.class);

    private long minTimestamp;

    protected SpanningTreeRSPQ(DeltaRSPQ<V> delta, V rootVertex, long timestamp) {
        this.rootNode = new TreeNodeRSPQ<V>(rootVertex, 0, null, this, timestamp);
        this.delta = delta;
        this.nodeIndex = HashMultimap.create();
        nodeIndex.put(Hasher.getTreeNodePairKey(rootVertex, 0), rootNode);
        this.markings = Sets.newHashSet();
        this.minTimestamp = timestamp;
    }


    public TreeNodeRSPQ<V> addNode(TreeNodeRSPQ parentNode, V childVertex, int childState, long timestamp) {
        if(parentNode == null) {
            // TODO no object found
        }
        if(parentNode.getTree().equals(this)) {
            // TODO wrong tree
        }

        TreeNodeRSPQ<V> child = new TreeNodeRSPQ<>(childVertex, childState, parentNode, this, timestamp);
        nodeIndex.put(Hasher.getTreeNodePairKey(childVertex, childState), child);

        // a new node is added to the spanning tree. update delta index
        this.delta.addToTreeNodeIndex(this, child);

        this.updateTimestamp(timestamp);

        return child;
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.containsKey(Hasher.getTreeNodePairKey(vertex, state));
    }

    public Collection<TreeNodeRSPQ<V>>  getNodes(V vertex, int state) {
        Collection<TreeNodeRSPQ<V>> node = nodeIndex.get(Hasher.getTreeNodePairKey(vertex, state ));
        return node;
    }

    public V getRootVertex() {
        return this.rootNode.getVertex();
    }

    public TreeNodeRSPQ<V> getRootNode() {
        return this.rootNode;
    }

    protected void updateTimestamp(long timestamp) {
        if(timestamp < minTimestamp) {
            this.minTimestamp = timestamp;
        }
    }

    /**
     * removes the node from current nodeIndex.
     * If there is no such node remaining, then remove it from delta tree index
     * @param node
     */
    private void removeNode(TreeNodeRSPQ<V> node) {
        Hasher.MapKey<V> nodeKey = Hasher.getTreeNodePairKey(node.getVertex(), node.getState());
        this.nodeIndex.remove(nodeKey, node);
        //remove this node from parent's chilren list
        node.setParent(null);
        if(this.nodeIndex.get(nodeKey).isEmpty()) {
            this.delta.removeFromTreeIndex(node, this);
        }
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void addMarking(V vertex, int state) {
        markings.add(Hasher.getTreeNodePairKey(vertex, state));
    }

    public boolean isMarked(V vertex, int state) {
        return markings.contains(Hasher.getTreeNodePairKey(vertex, state));
    }

    public void removeMarking(V vertex, int state) {
        markings.remove(Hasher.getTreeNodePairKey(vertex, state));
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
            currentNode = unmarkedNodes.pop();

            // get backward edges of the unmarked node
            Collection<GraphEdge<ProductGraphNode<V>>> backwardEdges = productGraph.getBackwardEdges(currentNode.getVertex(), currentNode.getState());
            for (GraphEdge<ProductGraphNode<V>> backwardEdge : backwardEdges) {
                V sourceVertex = backwardEdge.getSource().getVertex();
                int sourceState = backwardEdge.getSource().getState();
                // find all the nodes that are pruned due to previously marking
                Collection<TreeNodeRSPQ<V>> parentNodes = this.getNodes(sourceVertex, sourceState);
                for (TreeNodeRSPQ<V> parent : parentNodes) {
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

    /**
     * removes old edges from the productGraph, used during window management.
     * This function assumes that expired edges are removed from the productGraph, so traversal assumes that it is guarenteed to
     * traverse valid edges
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     * @return The set of nodes that have expired from the window as there is no other path
     */
    protected <L> void removeOldEdges(long minTimestamp, ProductGraph<V,L> productGraph) {
        // if root is expired (root node timestamp is its youngest edge), then the entire tree needs to be removed
//        if(this.rootNode.getTimestamp() <= minTimestamp) {
//            return this.nodeIndex.values();
//        }

        // potentially expired nodes
        HashSet<TreeNodeRSPQ<V>> candidates = new HashSet<>();
        HashSet<TreeNodeRSPQ<V>> candidateRemoval = new HashSet<>();

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
        //update the lowest minimum timestamp for this tree
        this.minTimestamp = minimumValidTimetamp;

        Iterator<TreeNodeRSPQ<V>> candidateIterator = candidates.iterator();
        HashSet<TreeNodeRSPQ> visited = new HashSet<>();

        LOG.debug("Expiry for spanning tree {}, # of candidates {} out of {} nodes", toString(), candidates.size(), nodeIndex.size());

        //scan over potential nodes once.
        // For each potential, check they have a valid non-tree edge in the original productGraph
        // If there is traverse down from here (in the productGraph) and remove all children from potentials
        while(candidateIterator.hasNext()) {
            TreeNodeRSPQ<V> candidate = candidateIterator.next();
            // check if a previous traversal already found a path for the candidate
            if(candidate.getTimestamp() > minTimestamp) {
                continue;
            }
            //check if there exists any incoming edge from a valid state
            Collection<GraphEdge<ProductGraphNode<V>>> backwardEdges = productGraph.getBackwardEdges(candidate.getVertex(), candidate.getState());
            TreeNodeRSPQ<V> newParent = null;
            GraphEdge<ProductGraphNode<V>> newParentEdge = null;
            for(GraphEdge<ProductGraphNode<V>> backwardEdge : backwardEdges) {
                Collection<TreeNodeRSPQ<V>> newParents = this.getNodes(backwardEdge.getSource().getVertex(), backwardEdge.getSource().getState());
                // candidate is a marked node, therefore these edges cannot form a cycle or register conflict
                for(TreeNodeRSPQ<V> newParentCandidate : newParents) {
                    if (!candidates.contains(newParentCandidate) || candidateRemoval.contains(newParentCandidate)) {
                        // there is an incoming edge with valid source
                        // source is valid (in the tree) and not in candidate
                        newParent = newParentCandidate;
                        newParentEdge = backwardEdge;
                        break;
                    }
                }
                if(newParentEdge != null) {
                    // a valid backward edge is found
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
                LinkedList<TreeNodeRSPQ<V>> traversalQueue = new LinkedList<>();

                // initial node is the current candidate, because now it is reachable
                traversalQueue.add(candidate);
                while(!traversalQueue.isEmpty()){
                    TreeNodeRSPQ<V> currentVertex = traversalQueue.remove();
                    visited.add(currentVertex);

                    Collection<GraphEdge<ProductGraphNode<V>>> forwardEdges = productGraph.getForwardEdges(currentVertex.getVertex(), currentVertex.getState());
                    // for each potential child
                    for(GraphEdge<ProductGraphNode<V>> forwardEdge : forwardEdges) {
                        // I can simply retrieve from the tree index because any node that is reachable are in tree index
                        Collection<TreeNodeRSPQ<V>> outgoingTreeNodes = this.getNodes(forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState());
                        for (TreeNodeRSPQ<V> outgoingTreeNode : outgoingTreeNodes) {
                            // there exists such node in the tree & the edge we are traversing is valid & this node has not been visited before
                            if (forwardEdge.getTimestamp() > minTimestamp && !visited.contains(outgoingTreeNode)) {
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

        }

        // update candidates
        candidates.removeAll(candidateRemoval);

        // now if there is any potential remanining, it is guarenteed that they are not reachable
        // so simply clean the indexes and generate negative result if necessary
        for(TreeNodeRSPQ<V> currentVertex : candidates) {
            // remove this node from the node index
            this.removeNode(currentVertex);
        }

        if(this.isExpired(minTimestamp)) {
            TreeNodeRSPQ<V> removedTuple = this.getRootNode();
            delta.removeTree(this);
        }

        LOG.debug("Spanning tree rooted at {}, remove {} nodes at timestamp {} ", getRootVertex(), candidates.size(), minTimestamp);
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
