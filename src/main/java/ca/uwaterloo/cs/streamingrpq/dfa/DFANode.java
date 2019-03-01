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
    private Set<SubPath> deletionBuffer;
    private Set<SubPath> insertionBuffer;

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
        this.deletionBuffer = new HashSet<>();
        this.insertionBuffer = new HashSet<>();

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
     * Runs an iteration of propagation. In each iteration, subpath in the queue are checked for extension, and extended paths are propagated to downstream nodes
     * @return true iteration resulted in subpaths to be propagated
     */
    public boolean iterate() {
        return false;
    }

    /**
     * Populates the insertion buffer for this NFA node
     * @param subPaths
     */
    public void propagateInsertion(Collection<SubPath> subPaths) {
        this.insertionBuffer.addAll(subPaths);
    }

    /**
     * Populates the deletion buffer for this NFA Node
     * @param subPaths
     */
    public void propagateDeletion(Collection<SubPath> subPaths) {
        this.deletionBuffer.addAll(subPaths);
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
            if(!(newPath.getSource().equals(subPath.getTarget()) && newPath.getSourceState().equals(subPath.getSource()))) {
                SubPath match = new SubPath(newPath.getSource(), subPath.getTarget(), newPath.getSourceState());
                target2Process.add(match);
            }
        }

        // we have match, we need to push it to downstream nodes for processing
        if (isDeletion) {
            downstreamNodes.stream().filter(downstreamNode -> downstreamNode.nodeId.equals(targetState)).forEach(downstreamNode -> downstreamNode.propagateDeletion(target2Process));
        } else {
            downstreamNodes.stream().filter(downstreamNode -> downstreamNode.nodeId.equals(targetState)).forEach(downstreamNode -> downstreamNode.propagateInsertion(target2Process));
        }
    }

    public void extendInsert(List<SubPath> subPaths, Integer originatingState) {
        // retrieve all the existing paths of this state that can extend the incoming path
        List<SubPath> subPathsToProcesses = new ArrayList<>();

        //TODO: need to check whether the buffer has duplicates, then remove duplicates before extension
        // so when 1-5 edge is added, on q1, there will be an edge transition coming from state 0, and subpath of extension of 4-4 to 4-1 coming from state 2.
        // it should be considered only once. so state 1 will get two 4-1 from state 0, and only one is considered. Similar to waveguide's delta-reduce

        for(SubPath subPath : subPaths) {
                List<SubPath> newPaths = cache.retrieveBySource(subPath.getTarget(), originatingState);
                newPaths.stream().forEach(newPath -> {
                    if(!(subPath.getSource().equals(newPath.getTarget()) && subPath.getSourceState().equals(this.getNodeId()))) {
                        SubPath extendedNewPath = new SubPath(subPath.getSource(), newPath.getTarget(), subPath.getSourceState());
                        // because we join these two paths, total number of paths is the multiplication
                        if(!cache.contains(extendedNewPath)) {
                            subPathsToProcesses.add(extendedNewPath);
                        }
                    }
                });
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
            removePaths.stream().forEach(newPath -> {
                if(!(subPath.getSource().equals(newPath.getTarget()) && subPath.getSourceState().equals(this.getNodeId()))) {
                    SubPath extendedRemovePath = new SubPath(subPath.getSource(), newPath.getTarget(), subPath.getSourceState());
                    if(!cache.contains(extendedRemovePath)) {
                        subPathsToProcesses.add(extendedRemovePath);
                    }
                }
            });
        }

        if(subPathsToProcesses.isEmpty()) {
            return;
        }

        processDelete(subPathsToProcesses);
    }

    protected void processInsert(List<SubPath> subPaths) {
        // add all the subPaths to the cache, if it exist in the cache it will return false, then remove the cyclic ones (because we do not want to traverse cycle again)
        List<SubPath> noDuplicateSubPaths = subPaths.stream()
                .filter(t -> !( t.getSource().equals(t.getTarget()) && t.getSourceState().equals(this.getNodeId()) ))
                .filter(t -> cache.insertOrIncrement(t)).collect(Collectors.toList());

        // we only need to extend paths that can start from the start state, so filter out paths that does not start from the start state
        List<SubPath> prefixSubPaths = noDuplicateSubPaths.stream().filter(t -> t.getSourceState().equals(0)).collect(Collectors.toList());

        // get rid of cycles if this is a final state as well

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

        List<SubPath> prefixSubPaths = removedSubPaths.stream().filter(t -> t.getSourceState().equals(0)).collect(Collectors.toList());

        if(prefixSubPaths.isEmpty()) {
            return;
        }

        // extendDelete to downstream nodes
        for(DFANode downstream: downstreamNodes) {
            downstream.extendDelete(prefixSubPaths, this.nodeId);
        }

    }
}
