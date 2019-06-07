package ca.uwaterloo.cs.streamingrpq.dfa;

import ca.uwaterloo.cs.streamingrpq.data.*;
import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2019-02-22.
 */
public class DFA<L> extends DFANode {

    public static final int EXPECTED_NODES = 50000000;
    public static final int EXPECTED_NEIGHBOURS = 12;


    private HashMultimap<L, DFAEdge<L>> dfaEdegs = HashMultimap.create();
    private HashMap<Integer, DFANode> dfaNodes = new HashMap<>();
    private Multimap<Integer, Integer> results = HashMultimap.create();

    // algoithm specific data structures
    private DFST delta = new DFST(2*EXPECTED_NODES, EXPECTED_NEIGHBOURS);
    private GraphEdges<ProductNode> edges = new GraphEdges<>(EXPECTED_NODES, EXPECTED_NEIGHBOURS);
    private Markings<Integer, ProductNode, RSPQTuple> markings = new Markings<>(EXPECTED_NODES, EXPECTED_NEIGHBOURS);
    private boolean[][] containmentMark;


    private Set<Integer> finalState = new HashSet<>();
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
        this.finalState.add(finalState);
        dfaNodes.get(finalState).setFinal(true);
        dfaNodes.get(finalState).addDownstreamNode(this);
    }

    public void processEdge(InputTuple<Integer, Integer, L> input) throws NoSpaceException {
        Queue<QueuePair<RSPQTuple, ProductNode>> queue = new LinkedList<>();

        Set<DFAEdge<L>> dfaEdges = dfaEdegs.get(input.getLabel());

        for(DFAEdge<L> edge : dfaEdges) {
            // for each such node, add raw edge to the edges
            ProductNode sourceNode = new ProductNode(input.getSource(), edge.getSource().getNodeId());
            ProductNode targetNode = new ProductNode(input.getTarget(), edge.getTarget().getNodeId());

            // update set of existing edges

            edges.addNeighbour(sourceNode, targetNode);

            // if source state is 0 -> create a single edge tuple and add it to the queue
            if(edge.getSource().getNodeId() == this.startState) {
                RSPQTuple tuple = new RSPQTuple(input.getSource(), sourceNode);
                queue.offer(new QueuePair<>(tuple, targetNode));
            }

            // query Delta to get all existing tuples that can be extended
            Collection<RSPQTuple> prefixes = delta.retrieveByTarget(sourceNode);
            for(RSPQTuple prefix : prefixes) {
                // extend the prefix path with the new edge
                queue.offer(new QueuePair<>(prefix, targetNode));
            }

        }


        while (!queue.isEmpty()) {
            QueuePair<RSPQTuple, ProductNode> candidate = queue.poll();
            RSPQTuple prefixPath = candidate.getTuple();
            ProductNode predecessor = candidate.getProductNode();

            Collection<QueuePair<RSPQTuple, ProductNode>> newCandidates = extendPrefixPath(prefixPath, predecessor);
            queue.addAll(newCandidates);
        }

    }

    private Collection<QueuePair<RSPQTuple, ProductNode>> extendPrefixPath(RSPQTuple prefixPath, ProductNode targetNode) throws NoSpaceException {

        ArrayDeque<QueuePair<RSPQTuple, ProductNode>> candidates = new ArrayDeque<>();

        if(prefixPath.containsCM(targetNode.getVertex(), targetNode.getState())) {
            // return as this is clearly a cycle
        } else if( !hasContainment(prefixPath.getFirstCM(targetNode.getVertex()), targetNode.getState()) ) {
            // unmark all parents of the prefix path
            candidates.addAll(unmark(prefixPath));
        } else if (markings.contains(prefixPath.getSource(), targetNode)) {
            // target node is marked, so extend it
            this.markings.addCrossEdge(prefixPath.getSource(), targetNode, prefixPath);
        } else {
            // check if Delta contains the target node. Any leaf added for the first time is added as a marked node.
            if(!delta.contains(targetNode)) {
                markings.addMarking(prefixPath.getSource(), targetNode);
            }

            // extend new tuple and add it to DFST
            RSPQTuple newPath = prefixPath.extend(targetNode);
            delta.addTuple(newPath);

            // add to result set of final node is there
            if(finalState.contains(targetNode.getState())) {
                results.put(newPath.getSource(), newPath.getTarget());
            }

            Collection<ProductNode> extensionEdges = edges.getNeighbours(targetNode);
            for(ProductNode extensionEdge : extensionEdges) {
                // extend the newly added tuple with an existing edge
                QueuePair<RSPQTuple, ProductNode> candidate = new QueuePair<>(newPath, extensionEdge);
                candidates.add(candidate);
            }
        }

        // return newly explored candidates for the next iteration
        return candidates;
    }

    private Collection<QueuePair<RSPQTuple, ProductNode>> unmark(RSPQTuple tuple) {
        ArrayDeque<QueuePair<RSPQTuple, ProductNode>> candidates = new ArrayDeque<>();

        while(tuple != null) {
            if(markings.contains(tuple.getSource(), tuple.getTargetNode())) {
                // all incoming cross-edges are extended
                Collection<RSPQTuple> crossEdges = markings.getCrossEdges(tuple.getSource(), tuple.getTargetNode());
                // remove the marking
                markings.removeMarking(tuple.getSource(), tuple.getTargetNode());
                for(RSPQTuple crossEdge : crossEdges) {
                    QueuePair<RSPQTuple, ProductNode> candidate = new QueuePair<>(crossEdge, tuple.getTargetNode());
                    candidates.add(candidate);
                }
            }
            tuple = tuple.getParentNode();
        }

        return candidates;
    }

    private boolean hasContainment(Integer stateQ, Integer stateT) {
        if(stateQ == null) {
            return true;
        }
        return !this.containmentMark[stateQ][stateT];
    }

    /**
     * Optimization procedure for the autamaton, including minimization, containment relationship
     * MUST be called after automaton is constructed
     */
    public void optimize() {
        this.containmentMark = new boolean[dfaNodes.size()][dfaNodes.size()];
        int alphabetSize = dfaEdegs.keySet().size();

        // once we construct the minimized DFA, we can easily compute the sufflix language containment relationship
        // Algorithm S of Wood'95
        Map<StatePair, List<StatePair>> stateLists = new HashMap<>();
        for(int s = 0; s < dfaNodes.size(); s++) {
            for (int t = 0; t < dfaNodes.size(); t++) {
                stateLists.put(StatePair.createInstance(s,t), new ArrayList<>());
            }
        }
        // first create a transition matrix for the DFA
        int[][] transitionMatrix = new int[dfaNodes.size()][alphabetSize];
        for(int i = 0; i < dfaNodes.size(); i++) {
            for(int j = 0; j < alphabetSize; j++) {
                transitionMatrix[i][j] = -1;
            }
        }
        Iterator<L> edgeIterator = dfaEdegs.keySet().iterator();
        for(int j = 0 ; j < alphabetSize; j++) {
            Set<DFAEdge<L>> edges = dfaEdegs.get(edgeIterator.next());
            for (DFAEdge<L> edge : edges) {
                transitionMatrix[edge.getSource().getNodeId()][j] = edge.getTarget().getNodeId();
            }
        }

        // initialize: line 1 of Algorithm S
        for(int s = 0; s < dfaNodes.size(); s++) {
            for (int t = 0; t < dfaNodes.size(); t++) {
                // for s \in S-F and t \in F
                if(!finalState.contains(s) && finalState.contains(t)) {
                    containmentMark[s][t] = true;
                }
            }
        }

        // line 2-7 of Algorithm S0
        for(int s = 0; s < dfaNodes.size(); s++) {
            for (int t = 0; t < dfaNodes.size(); t++) {
                // for s,t \in ((SxS) - ((S-F)xF))
                if(finalState.contains(s) || !finalState.contains(t)) {
                    // implement line 3,
                    boolean isMarked = false;
                    Queue<StatePair> markQueue = new ArrayDeque<>();
                    for(int j = 0; j < alphabetSize; j++) {
                        if(transitionMatrix[s][j] == transitionMatrix[t][j] && transitionMatrix[s][j] != -1) {
                            isMarked = true;
                            markQueue.add(StatePair.createInstance(s,t));
                        }
                    }

                    // recursively mark all the pairs on the list of pairs that are marked in this step
                    // line 5 of the Algorithm S
                    while(!markQueue.isEmpty()) {
                        StatePair pair = markQueue.poll();
                        List<StatePair> pairList = stateLists.get(pair);
                        for(StatePair candidate : pairList) {
                            if(!containmentMark[candidate.stateS][candidate.stateT]) {
                                markQueue.add(candidate);
                                containmentMark[candidate.stateS][candidate.stateT] = true;
                            }
                        }
                    }

                    // if there is no marked, then populate the lists
                    // line 6 of Algorithm S
                    if(!isMarked) {
                        for(int j = 0; j < alphabetSize; j++) {
                            int sEndpoint = transitionMatrix[s][j];
                            int tEndpoint = transitionMatrix[t][j];
                            if(sEndpoint != -1 && tEndpoint != -1 && sEndpoint != tEndpoint) {
                                // Line 7 of Algorithm S
                                stateLists.get(StatePair.createInstance(sEndpoint, tEndpoint)).add(StatePair.createInstance(s,t));
                            }
                        }
                    }

                }
            }
        }

    }

    // TODO: implementations of InsertRAPQ and DeleteRAPQ

    public int getResultCounter() {
        return results.size();
    }

    public Collection<Pair<Integer, Integer>> getResults() {
        return results.entries().stream().map(e -> Pair.of(e.getKey(), e.getValue())).collect(Collectors.toList());
    }

    public int getGraphEdgeCount() {
        return edges.getEdgeCount();
    }

    public int getDeltaTupleCount() {
        return delta.getTupleCount();
    }
}
