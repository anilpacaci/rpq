package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.data.Tuple;
import ca.uwaterloo.cs.streamingrpq.core.SubPathExtension;
import ca.uwaterloo.cs.streamingrpq.data.Delta;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFANode {

    private List<DFANode> downstreamNodes;

    private Integer nodeId;

    private Delta delta;
    private Delta edges;
    // assumption is that we do edge at a time processing, so it is either all insert or all delete
    protected List<Tuple> processBuffer;
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
        this.delta = new Delta<Tuple>();
        this.edges = new Delta<Tuple>();
        this.processBuffer = new ArrayList<>();
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
     * @param tuples
     */
    public void pushToProcessBuffer(Collection<Tuple> tuples) {
        this.processBuffer.addAll(tuples);
    }

    /**
     * Populates the deletion buffer for this NFA Node
     * @param tuples
     */
    public void pushToExtendBuffer(Collection<Tuple> tuples, Integer originatingState) {
        tuples.stream().forEach(s -> this.extendBuffer.add(new SubPathExtension(s, originatingState)));
    }

    /**
     * Only to be called by the main program, on the source vertex of the state transition in the DFA
     * @param tuple
     * @param targetState
     */
    public void processEdge(Tuple tuple, Integer targetState, boolean isDeletion) {
        // retrieve all existing paths of this state and see it can be extended
        // we are only extending states starting from source state to save memory
        List<Tuple> newPaths = delta.retrieveByTargetAndTargetState(0, tuple.getSource());

        // do not extend cycles
        newPaths = newPaths.stream().filter(s -> !s.getSource().equals(s.getTarget())).collect(Collectors.toList());
        List<Tuple> target2Process = new ArrayList<>(newPaths.size() + 1);
        target2Process.add(tuple);
        for (Tuple newPath : newPaths) {
            Tuple match = new Tuple(newPath.getSource(), tuple.getTarget(), newPath.getTargetState());
            target2Process.add(match);
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
        List<Tuple> subPathsToProcesses = new ArrayList<>();

        for(SubPathExtension subPathExtension : this.extendBuffer) {
            Tuple tuple = subPathExtension.getTuple();
            Integer originatingState = subPathExtension.getOriginatingState();
            if(tuple.getSource().equals(tuple.getTarget())) {
                // do not extend cycles
                continue;
            }
            List<Tuple> removePaths = delta.retrieveBySource(tuple.getTarget(), originatingState);
            //eliminate cycles, then add new emerging ones that do not exists before
            removePaths.stream().forEach(newPath -> {
                Tuple extendedRemovePath = new Tuple(tuple.getSource(), newPath.getTarget(), tuple.getTargetState());
                subPathsToProcesses.add(extendedRemovePath);
            });
        }

        this.pushToProcessBuffer(subPathsToProcesses);
        this.extendBuffer.clear();

        return !subPathsToProcesses.isEmpty();
    }

    /**
     * Process all the extended subpaths and propagates the newly added/deleted ones
     * @param isDeletion
     * @return true if there is a new insertion/deletion to state delta, and new subpath is propagated
     */
    public boolean process(boolean isDeletion) {
        // add all the subPaths to the delta, if it exist in the delta it will return false, then remove the cyclic ones (because we do not want to traverse cycle again)
        List<Tuple> removedTuples;
        if(isDeletion) {
            removedTuples = this.processBuffer.stream().filter(t -> delta.removeOrDecrement(t)).collect(Collectors.toList());
        } else {
            removedTuples = this.processBuffer.stream().filter(t -> delta.insertOrIncrement(t)).collect(Collectors.toList());
        }
        Set<Tuple> prefixTuples = removedTuples.stream().filter(t -> t.getTargetState().equals(0)).collect(Collectors.toSet());

        // extendDelete to downstream nodes
        for(DFANode downstream: downstreamNodes) {
            downstream.pushToExtendBuffer(prefixTuples, this.nodeId);
        }

        this.processBuffer.clear();

        return !prefixTuples.isEmpty();
    }

    /**
     * Merge delta delta to the state delta at the end
     */
    public void mergeDelta() {
        List<Tuple> deltaPaths = deltaCache.retrieveAll();
        deltaPaths.stream().forEach(s -> {
            s.setCounter(1); delta.insertOrIncrement(s);
        });
        deltaCache.removeAll();
    }
}
