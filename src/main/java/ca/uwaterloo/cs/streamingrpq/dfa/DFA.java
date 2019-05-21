package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.data.Delta;
import ca.uwaterloo.cs.streamingrpq.data.ProductNode;
import ca.uwaterloo.cs.streamingrpq.data.QueuePair;
import ca.uwaterloo.cs.streamingrpq.data.Tuple;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * Created by anilpacaci on 2019-02-22.
 */
public class DFA<L> extends DFANode {

    private HashMultimap<L, DFAEdge<L>> dfaEdegs = HashMultimap.create();
    private HashMap<Integer, DFANode> dfaNodes = new HashMap<>();
    private HashSet<Tuple> results = new HashSet<>();
    private Delta delta = new Delta();
    private Multimap<ProductNode, ProductNode> edges = HashMultimap.create();
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

    public void processEdge(InputTuple<Integer, Integer, L> input) {
        Queue<QueuePair> queue = new LinkedList<>();

        Set<DFAEdge<L>> dfaEdges = dfaEdegs.get(input.getLabel());

        for(DFAEdge<L> edge : dfaEdges) {
            // for each such node, add raw edge to the edges
            ProductNode sourceNode = new ProductNode(input.getSource(), edge.getSource().getNodeId());
            ProductNode targetNode = new ProductNode(input.getTarget(), edge.getTarget().getNodeId());

            // if source state is 0 -> create a single edge tuple and add it to the queue
            if(edge.getSource().getNodeId() == this.startState) {
                Tuple tuple = new Tuple(input.getSource(), input.getTarget(), edge.getTarget().getNodeId());
                queue.offer(new QueuePair(tuple, sourceNode));
            }

            // query Delta to get all existing tuples that can be extended
            List<Tuple> prefixes = delta.retrieveByTargetAndTargetState(sourceNode.getVertex(), sourceNode.getState());
            for(Tuple prefix : prefixes) {
                // extend the prefix path with the new edge
                Tuple candidate = new Tuple(prefix.getSource(), targetNode.getVertex(), targetNode.getState());
                queue.offer(new QueuePair(candidate, sourceNode));
            }

        }

        if (input.isDeletion()) {

        }
        else {
            while (!queue.isEmpty()) {
                QueuePair candidate = queue.poll();
                Tuple candidateTuple = candidate.getTuple();
                ProductNode predecessor = candidate.getProductNode();
                if (!delta.contains(candidateTuple)) {
                    if(candidateTuple.getTargetState().equals(finalState)) {
                        // a new result
                    }
                    delta.add(candidateTuple);
                    ProductNode targetNode = new ProductNode(candidateTuple.getTarget(), candidateTuple.getTargetState());
                    Collection<ProductNode> extensionEdges = edges.get(targetNode);
                    for(ProductNode extensionEdgeTarget : extensionEdges) {
                        // extend the newly added tuple with an existing edge
                        Tuple tuple = new Tuple(candidateTuple.getSource(), extensionEdgeTarget.getVertex(), extensionEdgeTarget.getState());
                        queue.offer(new QueuePair(tuple, targetNode));
                    }
                }
                // add predecessor edge to the tuple in the delta
                delta.addPredecessor(candidateTuple, predecessor);
            }

        }
    }

    // TODO: implementations of InsertRAPQ and DeleteRAPQ

    public int getResultCounter() {
        return results.size();
    }

    public Collection<Tuple> getResults() {
        return results;
    }
}
