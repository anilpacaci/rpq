package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.data.*;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * Created by anilpacaci on 2019-02-22.
 */
public class DFA<L> extends DFANode {

    public static final int EXPECTED_NODES = 10000000;
    public static final int EXPECTED_NEIGHBOURS = 12;


    private HashMultimap<L, DFAEdge<L>> dfaEdegs = HashMultimap.create();
    private HashMap<Integer, DFANode> dfaNodes = new HashMap<>();
    private HashSet<Tuple> results = new HashSet<>();
    private Delta delta = new Delta(10000000, 12);
    private GraphEdges<ProductNode> edges = new GraphEdges<>(EXPECTED_NODES, EXPECTED_NEIGHBOURS);
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

            // update set of existing edges

            edges.addNeighbour(sourceNode, targetNode);

            // if source state is 0 -> create a single edge tuple and add it to the queue
            if(edge.getSource().getNodeId() == this.startState) {
                Tuple tuple = new Tuple(input.getSource(), targetNode);
                queue.offer(new QueuePair(tuple, sourceNode));
            }

            // query Delta to get all existing tuples that can be extended
            Collection<Integer> prefixes = delta.retrieveByTarget(sourceNode);
            for(Integer source : prefixes) {
                // extend the prefix path with the new edge
                Tuple candidate = new Tuple(source, targetNode);
                queue.offer(new QueuePair(candidate, sourceNode));
            }

        }


        while (!queue.isEmpty()) {
            QueuePair candidate = queue.poll();
            Tuple candidateTuple = candidate.getTuple();
            ProductNode predecessor = candidate.getProductNode();

            if (!delta.contains(candidateTuple)) {
                if(candidateTuple.getTargetState() == finalState) {
                    // new result
                    results.add(candidateTuple);
                }

                delta.addTuple(candidateTuple);

                Collection<ProductNode> extensionEdges = edges.getNeighbours(candidateTuple.getTargetNode());

                for(ProductNode extensionEdgeTarget : extensionEdges) {
                    // extend the newly added tuple with an existing edge
                    Tuple tuple = new Tuple(candidateTuple.getSource(), extensionEdgeTarget);
                    queue.offer(new QueuePair(tuple, candidateTuple.getTargetNode()));
                }
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
