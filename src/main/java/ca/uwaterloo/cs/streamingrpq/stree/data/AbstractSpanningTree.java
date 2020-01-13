package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class AbstractSpanningTree<V> {
    protected final Logger LOG = LoggerFactory.getLogger(SpanningTreeRAPQ.class);

    protected AbstractTreeNode<V> rootNode;
    protected Delta<V> delta;

    protected Multimap<Hasher.MapKey<V>, AbstractTreeNode<V>> nodeIndex;

    protected long minTimestamp;

    //expiry related data structures
    protected HashSet<AbstractTreeNode<V>> candidates;
    protected HashSet<AbstractTreeNode<V>> candidateRemoval;
    protected HashSet<AbstractTreeNode<V>> visited;

    protected AbstractSpanningTree(long timestamp, Delta<V> delta) {
        this.minTimestamp = timestamp;
        this.nodeIndex = HashMultimap.create(Constants.EXPECTED_TREE_SIZE, Constants.EXPECTED_LABELS);
        this.delta = delta;

        candidates = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        candidateRemoval = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        visited = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
    }

    public int getSize() {
        return nodeIndex.size();
    }

    /**
     * This function traverses down the entire tree and identify nodes with expired timestamp
     * Meanwhile, it finds the smallest timestamp valid timestamp to update the timestamp of tree
     * @param minTimestamp
     * @return min timestamp that is larger than the current min
     */
    protected abstract long populateCandidateRemovals(long minTimestamp);

    public AbstractTreeNode<V> addNode(TreeNode parentNode, V childVertex, int childState, long timestamp) {
        if(parentNode == null) {
            // TODO no object found
        }
        if(parentNode.getTree().equals(this)) {
            // TODO wrong tree
        }


        AbstractTreeNode child = delta.getObjectFactory().createTreeNode(this, childVertex, childState, parentNode, timestamp);
        nodeIndex.put(Hasher.createTreeNodePairKey(childVertex, childState), child);

        // a new node is added to the spanning tree. update delta index
        this.delta.addToTreeNodeIndex(this, child);

        this.updateTimestamp(timestamp);

        return child;
    }

    /**
     * removes the node from current nodeIndex.
     * If there is no such node remaining, then remove it from delta tree index
     * @param node
     */
    protected void removeNode(AbstractTreeNode<V> node) {
        Hasher.MapKey<V> nodeKey = Hasher.getThreadLocalTreeNodePairKey(node.getVertex(), node.getState());
        this.nodeIndex.remove(nodeKey, node);
        //remove this node from parent's chilren list
        node.setParent(null);
        if(this.nodeIndex.get(nodeKey).isEmpty()) {
            this.delta.removeFromTreeIndex(node, this);
        }
    }

    /**
     * removes old edges from the productGraph, used during window management.
     * This function assumes that expired edges are removed from the productGraph, so traversal assumes that it is guarenteed to
     * traverse valid edges
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     * @return The set of nodes that have expired from the window as there is no other path
     */
    public <L> Collection<AbstractTreeNode<V>> removeOldEdges(long minTimestamp, ProductGraph<V,L> productGraph) {
        // if root is expired (root node timestamp is its youngest edge), then the entire tree needs to be removed
//        if(this.rootNode.getTimestamp() <= minTimestamp) {
//            return this.nodeIndex.values();
//        }

        // potentially expired nodes
        candidates.clear();
        candidateRemoval.clear();


        //update the lowest minimum timestamp for this tree
        this.minTimestamp = populateCandidateRemovals(minTimestamp);

        Iterator<AbstractTreeNode<V>> candidateIterator = candidates.iterator();
        visited.clear();

        LOG.debug("Expiry for spanning tree {}, # of candidates {} out of {} nodes", toString(), candidates.size(), nodeIndex.size());

        //scan over potential nodes once.
        // For each potential, check they have a valid non-tree edge in the original productGraph
        // If there is traverse down from here (in the productGraph) and remove all children from potentials
        while(candidateIterator.hasNext()) {
            AbstractTreeNode<V> candidate = candidateIterator.next();
            // check if a previous traversal already found a path for the candidate
            if(candidate.getTimestamp() > minTimestamp) {
                continue;
            }
            //check if there exists any incoming edge from a valid state
            Collection<GraphEdge<ProductGraphNode<V>>> backwardEdges = productGraph.getBackwardEdges(candidate.getVertex(), candidate.getState());
            AbstractTreeNode<V> newParent = null;
            GraphEdge<ProductGraphNode<V>> newParentEdge = null;
            for(GraphEdge<ProductGraphNode<V>> backwardEdge : backwardEdges) {
                Collection<AbstractTreeNode<V>> newParents = this.getNodes(backwardEdge.getSource().getVertex(), backwardEdge.getSource().getState());
                // candidate is a marked node, therefore these edges cannot form a cycle or register conflict
                for(AbstractTreeNode<V> newParentCandidate : newParents) {
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
                LinkedList<AbstractTreeNode<V>> traversalQueue = new LinkedList<>();

                // initial node is the current candidate, because now it is reachable
                traversalQueue.add(candidate);
                while(!traversalQueue.isEmpty()){
                    AbstractTreeNode<V> currentVertex = traversalQueue.remove();
                    visited.add(currentVertex);

                    Collection<GraphEdge<ProductGraphNode<V>>> forwardEdges = productGraph.getForwardEdges(currentVertex.getVertex(), currentVertex.getState());
                    // for each potential child
                    for(GraphEdge<ProductGraphNode<V>> forwardEdge : forwardEdges) {
                        // I can simply retrieve from the tree index because any node that is reachable are in tree index
                        Collection<AbstractTreeNode<V>> outgoingTreeNodes = this.getNodes(forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState());
                        for (AbstractTreeNode<V> outgoingTreeNode : outgoingTreeNodes) {
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
        for(AbstractTreeNode<V> currentVertex : candidates) {
            // remove this node from the node index
            this.removeNode(currentVertex);
        }

        if(this.isExpired(minTimestamp)) {
            AbstractTreeNode<V> removedTuple = this.getRootNode();
            delta.removeTree(this);
        }

        LOG.debug("Spanning tree rooted at {}, remove {} nodes at timestamp {} ", getRootVertex(), candidates.size(), minTimestamp);

        return candidates;
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    public Collection<AbstractTreeNode<V>> getNodes(V vertex, int state) {
        Collection<AbstractTreeNode<V>> nodes = nodeIndex.get(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
        return nodes;
    }

    public V getRootVertex() {
        return this.rootNode.getVertex();
    }

    public AbstractTreeNode<V> getRootNode() {
        return this.rootNode;
    }

    protected void updateTimestamp(long timestamp) {
        if(timestamp < minTimestamp) {
            this.minTimestamp = timestamp;
        }
    }

    public long getMinTimestamp() {
        return minTimestamp;
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
