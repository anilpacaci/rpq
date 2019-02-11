package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.core.Tuple;
import ca.uwaterloo.cs.streamingrpq.util.Cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class DFANode {

    private List<DFANode> downstreamNodes;
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
        this.cache = new Cache();
        this.queue = new LinkedList<Tuple>();

        this.upstreamNodes = new ArrayList<>();
        this.downstreamNodes = new ArrayList<>();
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void addDownstreamNode(DFANode downstreamNode) {
        this.downstreamNodes.add(downstreamNode);
    }

    public void addUpstreamNode(DFANode upstreamNode) {
        this.upstreamNodes.add(upstreamNode);
    }

    public void prepend(Tuple tuple, Integer targetState) {
        // retrieve all existing paths of this state and see it can be extended
        List<Tuple> newPaths = cache.retrieveByTarget(tuple.getSource());
        for(Tuple newPath : newPaths) {
            Tuple match = new Tuple(newPath.getSource(), tuple.getTarget(), newPath.getSourceState());
            // we have match, we need to push it to upstream nodes for processing
            upstreamNodes.stream().filter(upstreamNode -> upstreamNode.nodeId == targetState).forEach(upstreamNode -> upstreamNode.process(match));
        }
    }

    public void append(Tuple tuple, Integer originatingState) {
        // retrieve all the existing paths of this state that can extend the incoming path
        List<Tuple> newPaths = cache.retrieveBySource(tuple.getTarget(), originatingState);
        for(Tuple newPath: newPaths) {
            process(new Tuple(tuple.getSource(), newPath.getTarget(), tuple.getSourceState()));
        }

    }

    public void process(Tuple tuple) {
        //check incoming tuple has already been processes
        if(cache.contains(tuple)) {
            // this tuple already exists so move on
            return;
        }

        // add this new tuple to cache
        cache.put(tuple);
        if(this.isFinal && tuple.getSourceState() == 0) {
            //System.out.println(tuple.getSource() + " " + tuple.getTarget());
            resultCounter++;
        }

        // append to upstream nodes
        for(DFANode upstream: upstreamNodes) {
            upstream.append(tuple, this.nodeId);
        }

    }

    public int getResultCounter() {
        return resultCounter;
    }
}
