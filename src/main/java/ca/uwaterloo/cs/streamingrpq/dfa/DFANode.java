package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.core.Tuple;
import ca.uwaterloo.cs.streamingrpq.util.Cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFANode {

    private List<DFANode> upstreamNodes;

    private Integer nodeId;

    private Cache cache;
    private Queue<Tuple> queue;

    private boolean isFinal;

    private int resultCounter;

    public DFANode(Integer nodeId) {
        this(nodeId, false);
    }

    public DFANode(Integer nodeId, boolean isFinal) {
        this.nodeId = nodeId;
        this.isFinal = isFinal;
        this.cache = new Cache<Tuple>();
        this.queue = new LinkedList<Tuple>();

        this.upstreamNodes = new ArrayList<>();
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void addUpstreamNode(DFANode upstreamNode) {
        this.upstreamNodes.add(upstreamNode);
    }

    /**
     * Only to be called by the main program, on the source vertex of the state transition in the DFA
     * @param tuple
     * @param targetState
     */
    public void prepend(Tuple tuple, Integer targetState) {
        // retrieve all existing paths of this state and see it can be extended
        // we are only extending states starting from source state to save memory
        List<Tuple> newPaths = cache.retrieveBySourceStateAndTarget(0, tuple.getSource());

        List<Tuple> target2Process = new ArrayList<>(newPaths.size() + 1);
        target2Process.add(tuple);
        for(Tuple newPath : newPaths) {
            Tuple match = new Tuple(newPath.getSource(), tuple.getTarget(), newPath.getSourceState());
            target2Process.add(match);
        }

        // we have match, we need to push it to upstream nodes for processing
        upstreamNodes.stream().filter(upstreamNode -> upstreamNode.nodeId == targetState).forEach(upstreamNode -> upstreamNode.process(target2Process));
    }


    public void append(List<Tuple> tuples, Integer originatingState) {
        // retrieve all the existing paths of this state that can extend the incoming path
        List<Tuple> tuplesToProcess = new ArrayList<>();

        for(Tuple tuple : tuples) {
                List<Tuple> newPaths = cache.retrieveBySource(tuple.getTarget(), originatingState);
                newPaths.stream().forEach(newPath -> tuplesToProcess.add(new Tuple(tuple.getSource(), newPath.getTarget(), tuple.getSourceState())));
        }

        if(tuplesToProcess.isEmpty()) {
            return;
        }

        process(tuplesToProcess);
    }

    public void process(List<Tuple> tuples) {
        // add all the tuples to the cache, if it exist in the cache it will return false, then remove the cyclic ones (because we do not want to traverse cycle again)
        List<Tuple> noDuplicateTuples = tuples.stream().filter(t -> cache.put(t)).filter(t -> !t.getTarget().equals(t.getSource())).collect(Collectors.toList());

        // we only need to extend paths that can start from the start state, so filter out paths that does not start from the start state
        List<Tuple> prefixTuples = noDuplicateTuples.stream().filter(t -> t.getSourceState() == 0).collect(Collectors.toList());

        if(prefixTuples.isEmpty()) {
            return;
        }

        // append to upstream nodes
        for(DFANode upstream: upstreamNodes) {
            upstream.append(prefixTuples, this.nodeId);
        }

    }

    public int getResultCounter() {
        return cache.retrieveBySourceState(0).size();
    }

    public List<Tuple> getResults() {
        List<Tuple> results = cache.retrieveBySourceState(0);
        return results;
    }
}
