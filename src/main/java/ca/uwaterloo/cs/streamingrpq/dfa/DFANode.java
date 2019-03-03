package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.core.SubPath;
import ca.uwaterloo.cs.streamingrpq.core.SubPathExtension;
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
    // assumption is that we do edge at a time processing, so it is either all insert or all delete
    protected Set<SubPath> processBuffer;
    protected Set<SubPathExtension> extendBuffer;

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
        this.processBuffer = new HashSet<>();
        this.extendBuffer = new HashSet<>();

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
     * Populates the insertion buffer for this NFA node
     * @param subPaths
     */
    public void pushToProcessBuffer(Collection<SubPath> subPaths) {
        this.processBuffer.addAll(subPaths);
    }

    /**
     * Populates the deletion buffer for this NFA Node
     * @param subPaths
     */
    public void pushToExtendBuffer(Collection<SubPath> subPaths, Integer originatingState) {
        subPaths.stream().forEach(s -> this.extendBuffer.add(new SubPathExtension(s, originatingState)));
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
        downstreamNodes.stream().filter(downstreamNode -> downstreamNode.nodeId.equals(targetState)).forEach(downstreamNode -> downstreamNode.pushToProcessBuffer(target2Process));
    }


    /**
     * Extends the all the subpaths progapated into the buffer and schedules them for processing
     * @return true if there is an element pushed to process buffer, processing should continue
     */
    public boolean extend(boolean isDeletion) {
        // retrieve all the existing paths of this state that can extend the incoming path
        List<SubPath> subPathsToProcesses = new ArrayList<>();

        for(SubPathExtension subPathExtension : this.extendBuffer) {
            SubPath subPath= subPathExtension.getSubPath();
            Integer originatingState = subPathExtension.getOriginatingState();
            List<SubPath> removePaths = cache.retrieveBySource(subPath.getTarget(), originatingState);
            removePaths.stream().forEach(newPath -> {
                if(!(subPath.getSource().equals(newPath.getTarget()) && subPath.getSourceState().equals(this.getNodeId()))) {
                    SubPath extendedRemovePath = new SubPath(subPath.getSource(), newPath.getTarget(), subPath.getSourceState());
                    subPathsToProcesses.add(extendedRemovePath);
                }
            });
        }

        this.pushToProcessBuffer(subPathsToProcesses);
        this.extendBuffer.clear();

        return !subPathsToProcesses.isEmpty();
    }

    /**
     * Process all the extended subpaths and propagates the newly added/deleted ones
     * @param isDeletion
     * @return true if there is a new insertion/deletion to state cache, and new subpath is propagated
     */
    public boolean process(boolean isDeletion) {
        // add all the subPaths to the cache, if it exist in the cache it will return false, then remove the cyclic ones (because we do not want to traverse cycle again)
        List<SubPath> removedSubPaths;
        if(isDeletion) {
            removedSubPaths = this.processBuffer.stream().filter(t -> cache.removeOrDecrement(t)).collect(Collectors.toList());
        } else {
            removedSubPaths = this.processBuffer.stream().filter(t -> cache.insertOrIncrement(t)).collect(Collectors.toList());
        }
        List<SubPath> prefixSubPaths = removedSubPaths.stream().filter(t -> t.getSourceState().equals(0)).collect(Collectors.toList());

        // extendDelete to downstream nodes
        for(DFANode downstream: downstreamNodes) {
            downstream.pushToExtendBuffer(prefixSubPaths, this.nodeId);
        }

        this.processBuffer.clear();

        return !prefixSubPaths.isEmpty();
    }
}
