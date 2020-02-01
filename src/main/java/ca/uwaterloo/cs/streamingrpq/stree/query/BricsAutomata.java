package ca.uwaterloo.cs.streamingrpq.stree.query;

import ca.uwaterloo.cs.streamingrpq.runtime.SPARQLQueryRunner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import dk.brics.automaton.*;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Link;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import guru.nidi.graphviz.parse.Parser;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class BricsAutomata extends Automata<String> {

    private static Logger logger = LoggerFactory.getLogger(BricsAutomata.class);


    private static final String GRAPHVIZ_TRANSITION_LABEL = "label";

    private Map<Character, String> reverseLabelMappings;
    private Map<String, Character> labelMappings;

    private Automaton automaton;

    // query statistics
    private int nfaStates;
    private int nfaTransitions;
    private int dfaStates;
    private int dfaTransitions;
    private int minimalDFAStates;
    private int minimalDFATransitions;
    private int kleeneStarCount;
    private int alternationCount;
    private int predicateCount;

    public BricsAutomata(Automaton automaton, Map<String, Character> labelMappings) {
        super();
        this.labelMappings = labelMappings;
        reverseLabelMappings = Maps.newHashMap();
        // populate reverse label mappings
        for(Map.Entry<String, Character> label : labelMappings.entrySet()) {
            reverseLabelMappings.put(label.getValue(), label.getKey());
        }
        this.automaton = automaton;
    }

    public void finalize() {
        nfaStates = automaton.getNumberOfStates();
        nfaTransitions = automaton.getNumberOfTransitions();

        // determinize and minimize the given automaton
        automaton.determinize();
        dfaStates = automaton.getNumberOfStates();
        dfaTransitions = automaton.getNumberOfTransitions();

        automaton.minimize();
        minimalDFAStates = automaton.getNumberOfStates();
        minimalDFATransitions = automaton.getNumberOfTransitions();


        // perform a breadth first traversal of the transition graph in order to record all transitions
        // data structures for simple BFS traversal
        Set<State> visited = Sets.newHashSet();
        Queue<State> queue = Lists.newLinkedList();

        queue.offer(this.automaton.getInitialState());

        while(!queue.isEmpty()) {
            State state = queue.poll();
            visited.add(state);
            if(state.isAccept()) {
                addFinalState(getStateNumber(state));
            }

            // go over all transitions of the current state
            // since Automaton is deterministic, no need to iterate over epsilon edges
            for(Transition transition : state.getTransitions()) {
                State targetState = transition.getDest();
                // we only have character transitions, so retrieve min
                String label = reverseLabelMappings.get(transition.getMax());

                // retrieve or create mapping for this specific label
                Map<Integer, Integer> transitions = labelTransitions.computeIfAbsent(label, key -> Maps.newHashMap());

                transitions.put(getStateNumber(state), getStateNumber(targetState));
                // add target state to queue if it is not visited before
                if(!visited.contains(targetState)) {
                    queue.offer(targetState);
                }
            }
        }
        // now automaton states are assigned to a contigious range, and transitions are grouped by label
        // finally compute the containment relationship
        computeContainmentRelationship();
    }

    public void generateTransitiongraph(String filename) {
        try {
            MutableGraph g = new Parser().read(this.automaton.toDot());
            g.nodeAttrs();
            for(MutableNode node : g.nodes()) {
                for(Link link : node.links()) {
                    link.attrs().forEach(attribute -> {
                        if(attribute.getKey().equals(GRAPHVIZ_TRANSITION_LABEL)) {
                            char characterValue = StringEscapeUtils.unescapeJava(attribute.getValue().toString()).charAt(0);
                            attribute.setValue(reverseLabelMappings.get(characterValue));
                        }
                    } );
                }
            }

            Graphviz.fromGraph(g).totalMemory(240000000).width(800).render(Format.PNG).toFile(new File(filename));
        } catch (IOException e) {
            logger.error("Graphviz file could not be constructed: " + filename, e);
        }
    }

    public int getNfaStates() {
        return nfaStates;
    }

    public int getNfaTransitions() {
        return nfaTransitions;
    }

    public int getDfaStates() {
        return dfaStates;
    }

    public int getDfaTransitions() {
        return dfaTransitions;
    }

    public int getMinimalDFAStates() {
        return minimalDFAStates;
    }

    public int getMinimalDFATransitions() {
        return minimalDFATransitions;
    }

    public int getKleeneStarCount() {
        return kleeneStarCount;
    }

    public void setKleeneStarCount(int kleeneStarCount) {
        this.kleeneStarCount = kleeneStarCount;
    }

    public int getAlternationCount() {
        return alternationCount;
    }

    public void setAlternationCount(int alternationCount) {
        this.alternationCount = alternationCount;
    }

    public int getPredicateCount() {
        return predicateCount;
    }

    public void setPredicateCount(int predicateCount) {
        this.predicateCount = predicateCount;
    }
}
