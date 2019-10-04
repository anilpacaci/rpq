package ca.uwaterloo.cs.streamingrpq.stree.data;

import com.google.common.base.Verify;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

public class SpanningTree<V> {

    private TreeNode<V> rootNode;
    private Delta<V> delta;

    Table<V, Integer, TreeNode> nodeIndex;

    protected SpanningTree(Delta<V> delta, V rootVertex, long timestamp) {
        this.rootNode = new TreeNode<V>(rootVertex, 0, null, this, timestamp);
        this.delta = delta;
        this.nodeIndex = HashBasedTable.create();
        nodeIndex.put(rootVertex, 0, rootNode);
        this.delta.addToTreeNodeIndex(this, rootNode);
    }


    public void addNode(TreeNode parentNode, V childVertex, int childState, long timestamp) {
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

    /**
     * removes old edges from the graph, used during window management.
     * This function assumes that expired edges are removed from the graph, so traversal assumes that it is guarenteed to
     * traverse valid edges
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     * @return The set of nodes that have expired from the window as there is no other path
     */
    protected <L> HashSet<TreeNode> removeOldEdges(long minTimestamp, Graph<V,L> graph, QueryAutomata<L> automata) {
        // potentially expired nodes
        HashSet<TreeNode> candidates = new HashSet<TreeNode>();
        HashSet<TreeNode> candidateRemoval = new HashSet<>();

        // perform a bfs traversal on tree, no need for visited as it is a three
        LinkedList<TreeNode> queue = new LinkedList<>();
        queue.add(rootNode);
        while(!queue.isEmpty()) {
            // populate the queue with children
            TreeNode currentVertex = queue.remove();
            queue.addAll(currentVertex.getChildren());

            // check time timestamp to decide whether it is expired
            if(currentVertex.getTimestamp() <= minTimestamp) {
                candidates.add(currentVertex);
            }
        }

        Iterator<TreeNode> candidateIterator = candidates.iterator();
        HashSet<TreeNode> visited = new HashSet<>();

        //scan over potential nodes once.
        // For each potential, check they have a valid non-tree edge in the original graph
        // If there is traverse down from here (in the graph) and remove all children from potentials
        while(candidateIterator.hasNext()) {
            TreeNode<V> candidate = candidateIterator.next();
            // check if a previous traversal already found a path for the candidate
            if(candidate.getTimestamp() > minTimestamp) {
                continue;
            }
            //check if there exists any incoming edge from a valid state
            Collection<GraphEdge<V, L>> backwardEdges = graph.getBackwardEdges(candidate.getVertex());
            TreeNode<V> newParent = null;
            GraphEdge<V,L> newParentEdge = null;
            for(GraphEdge<V,L> backwardEdge : backwardEdges) {
                Integer incomingState = automata.getTransition(candidate.getState(), backwardEdge.getLabel());
                // if there is a state transition with that label
                if(incomingState != null) {
                    newParent = this.getNode(backwardEdge.getSource(), incomingState);
                    if (newParent != null && (!candidates.contains(newParent) || candidateRemoval.contains(newParent))) {
                        // there is an incoming edge with valid source
                        // source is valid (in the tree) and not in candidate
                        newParentEdge = backwardEdge;
                        break;
                    }
                }
            }

            //if this node becomes valid, just traverse everything down in the graph. If somehow I traverse an edge who would
            // be an incoming edge of some candidate, then it is removed from candidate so I never check it there.
            // If that edge is checked during incoming edge search, than it might be only examined again with a traversal which makes sure
            // that edge cannot be visited again. Therefore it is O(m)
            if(newParentEdge != null) {
                // means that there was a tree node that is not in the candidates but in the tree as a valid node
                candidate.setParent(newParent);
                candidate.setTimestamp(Long.min(newParent.getTimestamp(), newParentEdge.getTimestamp()));
                // current vertex has a valid incoming edge, so it needs to be removed from candidates
                candidateRemoval.add(candidate);

                //now traverse the graph down from this node, and remove any visited node from candidates
                LinkedList<TreeNode> traversalQueue = new LinkedList<TreeNode>();

                // initial node is the current candidate, because now it is reachable
                traversalQueue.add(candidate);
                while(!traversalQueue.isEmpty()){
                    TreeNode<V> currentVertex = traversalQueue.remove();
                    visited.add(currentVertex);

                    Collection<GraphEdge<V, L>> forwardEdges = graph.getForwardEdges(currentVertex.getVertex());
                    // for each potential child
                    for(GraphEdge<V,L> forwardEdge : forwardEdges) {
                        Integer outgoingState = automata.getTransition(currentVertex.getState(), forwardEdge.getLabel());
                        // make sure that transition exists
                        if (outgoingState != null) {
                            // I can simply retrieve from the tree index because any node that is reachable are in tree index
                            TreeNode<V> outgoingTreeNode = this.getNode(forwardEdge.getSource(), outgoingState);
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

}
