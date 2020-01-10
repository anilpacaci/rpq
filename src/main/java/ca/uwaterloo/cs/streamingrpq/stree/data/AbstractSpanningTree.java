package ca.uwaterloo.cs.streamingrpq.stree.data;

import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.DeltaRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.SpanningTreeRAPQ;
import ca.uwaterloo.cs.streamingrpq.stree.data.arbitrary.TreeNode;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import ca.uwaterloo.cs.streamingrpq.stree.util.Hasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class AbstractSpanningTree<V> {

    protected TreeNode<V> rootNode;
    protected DeltaRAPQ<V> deltaRAPQ;

    protected Map<Hasher.MapKey<V>, TreeNode> nodeIndex;

    protected final Logger LOG = LoggerFactory.getLogger(SpanningTreeRAPQ.class);

    protected long minTimestamp;

    //expiry related data structures
    protected HashSet<TreeNode<V>> candidates;
    protected HashSet<TreeNode<V>> candidateRemoval;
    protected HashSet<TreeNode<V>> visited;

    protected AbstractSpanningTree(DeltaRAPQ<V> deltaRAPQ, V rootVertex, long timestamp) {
        this.rootNode = new TreeNode<V>(rootVertex, 0, null, this, timestamp);
        this.deltaRAPQ = deltaRAPQ;
        this.nodeIndex = new HashMap<>(Constants.EXPECTED_TREE_SIZE);
        nodeIndex.put(Hasher.createTreeNodePairKey(rootVertex, 0), rootNode);
        this.minTimestamp = timestamp;

        candidates = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        candidateRemoval = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
        visited = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
    }

    protected int getSize() {
        return nodeIndex.size();
    }

    public TreeNode<V> addNode(TreeNode parentNode, V childVertex, int childState, long timestamp) {
        if(parentNode == null) {
            // TODO no object found
        }
        if(parentNode.getTree().equals(this)) {
            // TODO wrong tree
        }

        TreeNode<V> child = new TreeNode<>(childVertex, childState, parentNode, this, timestamp);
        nodeIndex.put(Hasher.createTreeNodePairKey(childVertex, childState), child);

        // a new node is added to the spanning tree. update delta index
        this.deltaRAPQ.addToTreeNodeIndex(this, child);

        this.updateTimestamp(timestamp);

        return child;
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    public TreeNode getNode(V vertex, int state) {
        TreeNode node = nodeIndex.get(Hasher.getThreadLocalTreeNodePairKey(vertex, state ));
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
}
