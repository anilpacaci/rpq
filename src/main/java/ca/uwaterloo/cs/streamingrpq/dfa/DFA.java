package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.core.SubPath;
import ca.uwaterloo.cs.streamingrpq.core.SubPathExtension;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import com.google.common.collect.HashMultimap;

import java.util.*;

/**
 * Created by anilpacaci on 2019-02-22.
 */
public class DFA<L> extends DFANode {

    private HashMultimap<L, DFAEdge<L>> dfaEdegs = HashMultimap.create();
    private HashMap<Integer, DFANode> dfaNodes = new HashMap<>();
    private HashSet<SubPath> results = new HashSet<>();
    private Integer finalState;
    private Integer startState;

    public void addDFAEdge(Integer source, Integer target, L label) {
        DFANode sourceNode = dfaNodes.get(source);
        DFANode targetNode = dfaNodes.get(target);
        if(sourceNode == null) {
            sourceNode = new DFANode(source);
            dfaNodes.put(source, sourceNode);
        }
        if(targetNode == null) {
            targetNode = new DFANode(target);
            dfaNodes.put(target, targetNode);
        }

        sourceNode.addDownstreamNode(targetNode);
        dfaEdegs.put(label, new DFAEdge<>(sourceNode, targetNode, label));
    }

    public void setStartState(Integer startState) {
        this.startState = startState;
    }

    public void setFinalState(Integer finalState) {
        this.finalState = finalState;
        dfaNodes.get(finalState).setFinal(true);
        dfaNodes.get(finalState).addDownstreamNode(this);
    }

    public void processEdge(InputTuple<Integer, Integer, L> tuple) {
        Set<DFAEdge<L>> edges = dfaEdegs.get(tuple.getLabel());

        for(DFAEdge<L> edge : edges) {
            // for each such node, push subPath for processing at the target node
            SubPath subPath = new SubPath(tuple.getSource(), tuple.getTarget(), edge.getSource().getNodeId());
            if(tuple.isDeletion()) {
                edge.getSource().processEdge(subPath, edge.getTarget().getNodeId(), true);
            } else {
                edge.getSource().processEdge(subPath, edge.getTarget().getNodeId(), false);
            }
        }

        boolean changed = true;

        while(changed) {
            // perform an iteration of process
            changed = dfaNodes.values().stream().map(dfaNode -> dfaNode.process(tuple.isDeletion())).reduce(false, (a,b) -> a || b);

            // if changed, perform iteration of extend
            if(changed) {
                changed = dfaNodes.values().stream().map(dfaNode -> dfaNode.extend(tuple.isDeletion())).reduce(false, (a,b) -> a || b);
            }

            // run extend on this DFA machine so that result set is updated
            this.extend(tuple.isDeletion());
        }

    }

    public boolean extend(boolean isDeletion) {
        this.extendBuffer.stream().forEach(s -> {
            if(s.getSubPath().getSourceState() == this.startState) {
                if(isDeletion) {
                    this.results.remove(s.getSubPath());
                } else {
                    this.results.add(s.getSubPath());
                }
            }
        });
        this.extendBuffer.clear();

        return false;
    }

    public int getResultCounter() {
        return results.size();
    }

    public Collection<SubPath> getResults() {
        return results;
    }
}
