package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.core.SubPath;
import ca.uwaterloo.cs.streamingrpq.util.Cache;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFANode {

    private List<DFANode> downstreamNodes;

    private Integer nodeId;

    private Cache cache;
    private Queue<SubPath> queue;

    private boolean isFinal;

    private int resultCounter;

    /**
     * generates an internal node with random id
     */
    public DFANode() {
        this(ThreadLocalRandom.current().nextInt(), false);
    }

    public DFANode(Integer nodeId) {
        this(nodeId, false);
    }

    public DFANode(Integer nodeId, boolean isFinal) {
        this.nodeId = nodeId;
        this.isFinal = isFinal;
        this.cache = new Cache<SubPath>();
        this.queue = new LinkedList<SubPath>();

        this.downstreamNodes = new ArrayList<>();
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public void addDownstreamNode(DFANode downstreamNode) {
        this.downstreamNodes.add(downstreamNode);
    }

    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }

    /**
     * Only to be called by the main program, on the source vertex of the state transition in the DFA
     * @param subPath
     * @param targetState
     */
    public void processEdge(SubPath subPath, Integer targetState, boolean isDeletion) {
        // retrieve all existing paths of this state and see it can be extended
        // we are only extending states starting from source state to save memory
        List<SubPath> newPaths = cache.retrieveBySourceStateAndTarget(0, subPath.getSource());

        List<SubPath> target2Process = new ArrayList<>(newPaths.size() + 1);
        target2Process.add(subPath);
        for (SubPath newPath : newPaths) {
            SubPath match = new SubPath(newPath.getSource(), subPath.getTarget(), newPath.getSourceState());
            target2Process.add(match);
        }

        // we have match, we need to push it to downstream nodes for processing
        if (isDeletion) {
            downstreamNodes.stream().filter(downstreamNode -> downstreamNode.nodeId == targetState).forEach(downstreamNode -> downstreamNode.processDelete(target2Process));
        } else {
            downstreamNodes.stream().filter(downstreamNode -> downstreamNode.nodeId == targetState).forEach(downstreamNode -> downstreamNode.processInsert(target2Process));
        }
    }

    public void extendInsert(List<SubPath> subPaths, Integer originatingState) {
        //detect cycles
        List<SubPath> noCycles = subPaths.stream().filter(t -> !t.getTarget().equals(t.getSource())).collect(Collectors.toList());
        // retrieve all the existing paths of this state that can extend the incoming path
        List<SubPath> subPathsToProcesses = new ArrayList<>();

        for(SubPath subPath : noCycles) {
                List<SubPath> newPaths = cache.retrieveBySource(subPath.getTarget(), originatingState);
                newPaths.stream().forEach(newPath -> subPathsToProcesses.add(new SubPath(subPath.getSource(), newPath.getTarget(), subPath.getSourceState())));
        }

        if(subPathsToProcesses.isEmpty()) {
            return;
        }

        processInsert(subPathsToProcesses);
    }

    public void extendDelete(List<SubPath> subPaths, Integer originatingState) {
        // retrieve all the existing paths of this state that can extend the incoming path
        List<SubPath> subPathsToProcesses = new ArrayList<>();

        for(SubPath subPath : subPaths) {
            List<SubPath> removePaths = cache.retrieveBySource(subPath.getTarget(), originatingState);
            removePaths.stream().forEach(newPath -> subPathsToProcesses.add(new SubPath(subPath.getSource(), newPath.getTarget(), subPath.getSourceState())));
        }

        if(subPathsToProcesses.isEmpty()) {
            return;
        }

        processDelete(subPathsToProcesses);
    }

    protected void processInsert(List<SubPath> subPaths) {
        // add all the subPaths to the cache, if it exist in the cache it will return false, then remove the cyclic ones (because we do not want to traverse cycle again)
        List<SubPath> noDuplicateSubPaths = subPaths.stream().filter(t -> cache.insertOrIncrement(t)).collect(Collectors.toList());

        // we only need to extend paths that can start from the start state, so filter out paths that does not start from the start state
        List<SubPath> prefixSubPaths = noDuplicateSubPaths.stream().filter(t -> t.getSourceState() == 0).collect(Collectors.toList());

        if(prefixSubPaths.isEmpty()) {
            return;
        }

        // extendInsert to downstream nodes
        for(DFANode downstream: downstreamNodes) {
            downstream.extendInsert(prefixSubPaths, this.nodeId);
        }
    }

    protected void processDelete(List<SubPath> subPaths) {
        // add all the subPaths to the cache, if it exist in the cache it will return false, then remove the cyclic ones (because we do not want to traverse cycle again)
        List<SubPath> removedSubPaths = subPaths.stream().filter(t -> cache.removeOrDecrement(t)).collect(Collectors.toList());

        if(removedSubPaths.isEmpty()) {
            return;
        }

        // extendDelete to downstream nodes
        for(DFANode downstream: downstreamNodes) {
            downstream.extendDelete(removedSubPaths, this.nodeId);
        }

    }
}
